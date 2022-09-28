from .mymanager import MyManager
import uuid
from multiprocessing.managers import SyncManager
import sys


class KafkaConsumer:
    def __init__(self, topic, bootstrap_servers, value_deserializer=None, mode='not_from_beginning'):
        self.mode = mode
        self.id = uuid.uuid4()
        self.topic = topic
        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(address=(ip, port))  # , authkey=b'supersecretauthkey')
        manager.connect()

        self.shared_dict = manager.get_queue_dict()
        self.topic_dict = manager.get_event_dict()
        self.offset_dict = manager.get_offset_dict()
        self.lock_dict = manager.get_lock_dict()

        self.n_msg_topic = len(self.shared_dict.get(topic))

        self.offset = 0

        self.event = SyncManager.Event(manager._manager)
        self.event.clear()

        if topic in self.topic_dict.keys():
            topic_dict = self.topic_dict.get(self.topic)
            topic_dict.update([(self.id, self.event)])
        else:
            print('"{}" is not a registered topic.'.format(self.topic))
            sys.exit(1)

    def __iter__(self):
        with self.lock_dict.get(self.topic):
            topic_offset = self.offset_dict.get(self.topic)
            self.event.clear()
        if self.mode == 'from_beginning':
            self.offset = max(0, topic_offset - self.n_msg_topic)
        else:
            self.offset = topic_offset
        return self

    def __next__(self):
        keep_waiting = True
        while keep_waiting:
            with self.lock_dict.get(self.topic):
                topic_offset = self.offset_dict.get(self.topic)
            if self.offset >= topic_offset:
                # print('Waiting')
                self.event.wait(timeout=None)
                self.event.clear()
            else:
                keep_waiting = False

        with self.lock_dict.get(self.topic):
            topic_offset = self.offset_dict.get(self.topic)
            idx = self.offset - topic_offset + self.n_msg_topic
            # print('idx={}, consumer offset={}, producer offset={}, n_msgs={}'.format(idx, self.offset, topic_offset, self.n_msg_topic))
            if idx >= self.n_msg_topic:
                print('Error: Index out of range. idx={}, n_msgs={}, consumer offset={}, topic offset={}'.format(idx, self.n_msg_topic, self.offset, topic_offset, topic_offset))
                msg = None
            else:
                msg = self.shared_dict.get(self.topic)[idx]


        # if idx >= self.n_msg_topic:
        #     print(idx, self.offset, self.offset_dict.get(self.topic), self.n_msg_topic)
        #     msg = self.shared_dict.get(self.topic)[idx - 1]
        # else:

        self.offset += 1

        kafka_msg = type('', (), {'value': msg})
        return kafka_msg  # self.offset]

    def __del__(self):
        print('Consumer stopped')
        self.lock_dict.get(self.topic).release()
        # super().__del__()
        # self.topic_dict.get(self.topic).pop(id)