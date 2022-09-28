from .mymanager import MyManager


class NQKafkaServer:
    def __init__(self, bootstrap_servers):
        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        self.manager = MyManager(server=True, address=(ip, port))

    def start(self):
        self.manager.start()

