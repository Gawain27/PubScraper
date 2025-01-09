import logging
import time
from datetime import datetime

from couchdb import ResourceConflict, ResourceNotFound, Unauthorized, ServerError, Database


class DatabaseHandler:
    def __init__(self, client, db_name, logger_name='DatabaseHandler'):
        self.client = client
        self.db_name = db_name
        self.logger = logging.getLogger(logger_name)
        self.db = self.get_or_create_db()

    def get_or_create_db(self) -> Database:
        try:
            db = self.client[self.db_name]
        except ResourceNotFound:
            self.logger.info(f"Database {self.db_name} not found. Creating new database.")
            db = self.client.create(self.db_name)
        except Unauthorized:
            self.logger.error("Unauthorized access to CouchDB. Check your credentials.")
            raise
        except ServerError as e:
            self.logger.error(f"Server error: {e}")
            raise
        return db

    def get_document(self, doc_id):
        try:
            return self.db.get(doc_id)
        except ResourceNotFound:
            self.logger.info(f"Document {doc_id} not found.")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching document {doc_id}: {e}")
            raise

    def insert_or_update_document(self, doc_type, doc_id, doc):
        doc['_id'] = doc_id
        doc['type'] = doc_type
        doc['update_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            try:
                existing_doc = self.get_document(doc_id)
                if existing_doc:
                    doc['update_count'] = doc['update_count'] + 1 if doc.__contains__('update_count') else 1
                    doc['_rev'] = existing_doc['_rev']
                self.db.save(doc)
                self.logger.info(f"Document of type {doc_type} with id {doc_id} saved successfully.")
                return

            except ResourceConflict:
                self.logger.warning(
                    f"Conflict encountered while saving document of type {doc_type} with id {doc_id}. Retrying ({retry_count + 1}/{max_retries}).")
                retry_count += 1
                time.sleep(5)

            except Exception as e:
                self.logger.error(f"Error saving document {doc_id}: {e}")
                raise

        # If max retries exceeded, raise an exception
        raise Exception(f"Failed to save document of type {doc_type} with id {doc_id} after {max_retries} retries.")
