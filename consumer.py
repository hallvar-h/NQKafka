from mymanager import MyManager
import time
import uuid
import multiprocessing as mp
import threading
from multiprocessing.managers import SyncManager
import sys


class KafkaConsumerLookalike:
    def __init__(self, topic, bootstrap_servers, value_deserializer=None):
        self.id = uuid.uuid4()
        self.topic = topic
        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(address=(ip, port), authkey=b'abracadabra')
        manager.connect()

        self.shared_dict = manager.get_queue_dict()
        self.topic_dict = manager.get_event_dict()

        self.event = SyncManager.Event(manager._manager)

        if topic in self.topic_dict.keys():
            topic_dict = self.topic_dict.get(topic)
            topic_dict.update([(self.id, self.event)])
        else:
            print('"{}" is not a registered topic.'.format(topic))
            sys.exit(1)

    def __iter__(self):
        return self

    def __next__(self):
        self.event.wait(timeout=None)
        self.event.clear()
        return self.shared_dict.get(self.topic)



if __name__ == '__main__':
    import socket
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    kafka_consumer = KafkaConsumerLookalike('time', bootstrap_servers=bootstrap_servers)

    for msg in kafka_consumer:
        print(msg)