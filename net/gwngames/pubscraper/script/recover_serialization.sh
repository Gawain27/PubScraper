#!/bin/bash

# CouchDB URL and database name
COUCHDB_URL="http://localhost:5984"
DATABASE="google_scholar"

# Temporary file to store query results
QUERY_RESULT="query_result.json"

# Step 1: Perform the Mango query to find matching documents
cat <<EOF > mango_query.json
{
  "selector": {
    "$or": [
      { "serialized": { "$exists": false } },
      { "serialized": true, "sent": false }
    ]
  },
  "fields": ["_id", "_rev"]
}
EOF

curl -X POST "$COUCHDB_URL/$DATABASE/_find" \
     -H "Content-Type: application/json" \
     -d @mango_query.json > $QUERY_RESULT

# Step 2: Parse results to prepare bulk update payload
BULK_UPDATE_PAYLOAD="bulk_update.json"

cat $QUERY_RESULT | jq '.docs |= map(. + {"serialized": false, "sent": false})' > $BULK_UPDATE_PAYLOAD

# Step 3: Perform bulk update
curl -X POST "$COUCHDB_URL/$DATABASE/_bulk_docs" \
     -H "Content-Type: application/json" \
     -d @$BULK_UPDATE_PAYLOAD

# Cleanup temporary files
rm mango_query.json $QUERY_RESULT $BULK_UPDATE_PAYLOAD

echo "Update completed."
