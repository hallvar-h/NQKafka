from ensurepip import bootstrap
from nqkafka import NQKafkaServer, KafkaProducer, KafkaConsumer
from nqkafka.utils import create_topic, stop_server
import multiprocessing as mp
import sys
import time
from nqkafka.mymanager import MyManager

class MyManagerMod(MyManager):
    def start(self):
        time.sleep(3)
        s = self._manager.get_server()
        s.serve_forever()


class NQKafkaServerMod(NQKafkaServer):
    @staticmethod
    def start_server(ip, port):
        manager = MyManagerMod(server=True, address=(ip, port))
        manager.start()


def run_server(bootstrap_servers):
    server = NQKafkaServerMod(bootstrap_servers)
    server.start()


def test():
    bootstrap_servers = 'localhost:40016'

    n_msgs = 20
    run_server(bootstrap_servers)
    # create_topic('time', bootstrap_servers=bootstrap_servers, n_samples=50)

    time.sleep(5)

    stop_server(bootstrap_servers)

    # sys.exit()


if __name__ == '__main__':
    test()