from .mymanager import MyManager
import multiprocessing as mp


class NQKafkaServer:
    def __init__(self, bootstrap_servers):
        self.ip, port_str = bootstrap_servers.split(':')
        self.port = int(port_str)
        self.server_process = mp.Process(target=self.run_server, args=(bootstrap_servers,))
        self.manager = MyManager(address=(self.ip, self.port))  # , authkey=b'supersecretauthkey')
        self.running = False

    @staticmethod
    def run_server(bootstrap_servers):
        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(server=True, address=(ip, port))
        manager.start()

    def start(self):
        self.server_process.start()
        self.manager.connect()
        self.shared_dict = self.manager.get_queue_dict()

        

