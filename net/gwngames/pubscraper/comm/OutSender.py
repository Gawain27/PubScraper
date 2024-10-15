from couchdb import Database

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.comm.SynchroSocket import SynchroSocket
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from net.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class OutSender:
    def __init__(self):
        server_port = JsonReader(JsonReader.CONFIG_FILE_NAME).get_value(ConfigConstants.SERVER_ENTITY_PORT)
        self.socket_instance = SynchroSocket(server_port)
        self.ctx = Context()

    def send_data(self, data: SendEntity):
        data_source: DatabaseHandler = DatabaseHandler(self.ctx.get_dbclient(), data.entity_db)
        database: Database = data_source.get_or_create_db()

        entity = database.get(data.entity_id)
        if entity is None:
            raise Exception("Entity found none for message %s", data.content)

        self.socket_instance.send_message(data.entity)

        entity['sent'] = True
        data_source.insert_or_update_document(data.content, data.entity_id, entity)

