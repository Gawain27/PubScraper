from net.gwngames.pubscraper.comm.SynchroSocket import SynchroSocket
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class OutSender:
    def __init__(self):
        server_port = JsonReader(JsonReader.CONFIG_FILE_NAME).get_value(ConfigConstants.SERVER_ENTITY_PORT)
        self.socket_instance = SynchroSocket(server_port)

    def send_data(self, data):
        self.socket_instance.send_message(data)
