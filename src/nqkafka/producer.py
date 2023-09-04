from .mymanager import MyManager


class KafkaProducer:
    def __init__(self, bootstrap_servers, value_serializer=None, *args, **kwargs):

        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(address=(ip, port))  # , authkey=b'supersecretauthkey')
        manager.connect()

        self.producer_queue = manager.get_producer_queue()

        # self.output_stream = manager.Queue()
        # self.init_queue.put('producer', self.output_stream)

    def send(self, topic, msg):
        try:
            self.producer_queue.put([topic, msg])
        except ConnectionResetError:
            # This means that server has stopped
            pass


    def flush(self):
        pass