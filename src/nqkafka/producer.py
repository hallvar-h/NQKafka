from .mymanager import MyManager
import time
import numpy as np


class KafkaProducer:
    def __init__(self, bootstrap_servers, value_serializer=None):

        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(address=(ip, port), authkey=b'supersecretauthkey')
        manager.connect()
        self.shared_dict = manager.get_queue_dict()
        self.offset_dict = manager.get_offset_dict()
        self.topic_dict = manager.get_event_dict()
        self.lock_dict = manager.get_lock_dict()

    def send(self, topic, msg):
        with self.lock_dict.get(topic):
            data = self.shared_dict.get(topic)
            data.pop(0)
            data.append(msg)
            offset = self.offset_dict.get(topic) + 1
            self.offset_dict.update([(topic, offset)])
            # print(data, offset)
            print(self.topic_dict.get(topic))

            for consumer_id, event in self.topic_dict.get(topic).items():
                event.set()
        # mark_for_deletion = []

        #     if event.is_set():
        #         mark_for_deletion.append(consumer_id)
        #     else:

        # for id in mark_for_deletion:
        #     self.topic_dict.get(topic).pop(id)
        #
        # print(self.topic_dict.get(topic))

    def flush(self):
        pass