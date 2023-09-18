from .mymanager import MyManager
import uuid
from multiprocessing.managers import SyncManager
import sys


class KafkaConsumer:
    def __init__(self, topic, bootstrap_servers, value_deserializer=None, mode='not_from_beginning', *args, **kwargs):
        self.mode = mode
        self.id = uuid.uuid4()
        self.topic = topic
        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(address=(ip, port))  # , authkey=b'supersecretauthkey')
        manager.connect()

        self.init_queue = manager.get_init_queue()
        self.input_stream = manager.Queue()
        self.recv_event = manager.Event()
        self.recv_event.clear()
        self.ready_for_msg_event = manager.Event()
        self.ready_for_msg_event.clear()
        self.init_queue.put(['consumer', self.id, topic, self.input_stream, self.recv_event, self.ready_for_msg_event, mode])
        
        
        # self.n_msg_topic = len(self.shared_dict.get(topic))

        self.offset = 0

        # if topic in self.topic_dict.keys():
            # topic_dict = self.topic_dict.get(self.topic)
            # topic_dict.update([(self.id, self.event)])
        # else:
            # print('"{}" is not a registered topic.'.format(self.topic))
            # sys.exit(1)

    def __iter__(self):
        # topic_offset = self.offset_dict.get(self.topic)

        # if self.mode == 'from_beginning':
            # self.offset = max(0, topic_offset - self.n_msg_topic)
        # else:
            # self.offset = topic_offset
        return self

    def __next__(self):
        
        # msg = self.input_queue.get()
        self.ready_for_msg_event.set()
        try:
            self.offset, msg = self.input_stream.get()
            self.recv_event.set()
            # self.input_stream.put(self.offset)
            # server_closed = False

            kafka_msg = type('KafkaMsg', (), {'value': msg})
            return kafka_msg  # self.offset]
        except ConnectionResetError:
            # This means that server has stopped
            server_closed = True
        
        if server_closed:
            raise StopIteration
       
        # topic_offset = self.offset_dict.get(self.topic)

        # if self.offset >= topic_offset:
        #     self.event.wait(timeout=None)
        #     self.event.clear()

        # topic_offset = self.offset_dict.get(self.topic)
        # idx = self.offset - topic_offset + self.n_msg_topic
        # # print('idx={}, consumer offset={}, producer offset={}, n_msgs={}'.format(idx, self.offset, topic_offset, self.n_msg_topic))
        # if idx >= self.n_msg_topic:
        #     print('Error: Index out of range. idx={}, n_msgs={}, consumer offset={}, topic offset={}'.format(idx, self.n_msg_topic, self.offset, topic_offset, topic_offset))
        #     msg = None
        #     msg = self.shared_dict.get(self.topic)[idx]

        # if idx >= self.n_msg_topic:
        #     print(idx, self.offset, self.offset_dict.get(self.topic), self.n_msg_topic)
        #     msg = self.shared_dict.get(self.topic)[idx - 1]
        # else:

        # self.offset += 1
        