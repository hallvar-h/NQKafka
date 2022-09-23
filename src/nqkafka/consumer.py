from .mymanager import MyManager
import time
import uuid
import multiprocessing as mp
import threading
from multiprocessing.managers import SyncManager
import sys


class KafkaConsumer:
    def __init__(self, topic, bootstrap_servers, value_deserializer=None, mode='from_beginning'):
        self.mode = mode
        self.id = uuid.uuid4()
        self.topic = topic
        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(address=(ip, port), authkey=b'abracadabra')
        manager.connect()

        self.shared_dict = manager.get_queue_dict()
        self.topic_dict = manager.get_event_dict()
        self.offset_dict = manager.get_offset_dict()
        self.lock_dict = manager.get_lock_dict()

        self.n_msg_topic = len(self.shared_dict.get(topic))

        self.offset = 0

        self.event = SyncManager.Event(manager._manager)

        if topic in self.topic_dict.keys():
            topic_dict = self.topic_dict.get(self.topic)
            topic_dict.update([(self.id, self.event)])
        else:
            print('"{}" is not a registered topic.'.format(self.topic))
            sys.exit(1)

    def __iter__(self):
        if self.mode == 'from_beginning':
            self.offset = self.offset_dict.get(self.topic) - self.n_msg_topic
        else:
            self.offset = self.offset_dict.get(self.topic)
        return self

    def __next__(self):
        if self.offset >= self.offset_dict.get(self.topic):
            self.event.wait(timeout=None)
            self.event.clear()
        idx = self.offset - self.offset_dict.get(self.topic) + self.n_msg_topic - 1
        # print(idx, self.offset, self.offset_dict.get(self.topic), self.n_msg_topic)
        self.offset += 1
        return self.shared_dict.get(self.topic)[idx]  # self.offset]

    def __del__(self):
        self.topic_dict.get(self.topic).pop(id)