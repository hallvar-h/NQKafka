from .mymanager import MyManager
import multiprocessing as mp
import time
import threading

class NQKafkaServer:
    def __init__(self, bootstrap_servers):
        self.ip, port_str = bootstrap_servers.split(':')
        self.port = int(port_str)
        self.data = {}
        # self.consumers = {}
        self.offsets = {}
        self.topic_lengths = {}
        self.topic_list = []
        self.consumer_thread_notifyers = {}
        self.topic_locks = {}
        
        self.server_process = mp.Process(target=self.start_server, args=(self.ip, self.port,))

        self.consumer_listener_thread = threading.Thread(target=self.consumer_listener)
        self.msg_listener_thread = threading.Thread(target=self.msg_listener)

    def consumer_listener(self):
        init_queue = self.manager.get_init_queue()
        while True:
            #print('Waiting for consumers...')
            try:
                msg = init_queue.get()
            except ConnectionResetError:
                print('Server closed.')
                break
            
            if msg[0] == 'consumer':
                id = msg[1]
                topic_name = msg[2]
                consumer_queue = msg[3]
                consumer_recv_event = msg[4]
                mode = msg[5]

                if not topic_name in self.topic_list:
                    print('"{}" is not a registered topic.'.format(topic_name))
                    break

                notifyer_event = threading.Event()
                notifyer_event.clear()

                event_dict = self.consumer_thread_notifyers[topic_name]
                event_dict.update([(id, notifyer_event)])

                topic_offset = self.offsets[topic_name]
                if mode == 'from_beginning':
                    consumer_offset = max(0, topic_offset - self.topic_lengths[topic_name])
                else:
                    consumer_offset = topic_offset
                # self.consumers[topic_name].append((consumer_offset, consumer_queue))

                new_consumer_server_thread = threading.Thread(target=self.serve_consumer, args=(topic_name, consumer_offset, consumer_queue, consumer_recv_event, notifyer_event))
                new_consumer_server_thread.start()
                # self.consumer_servers.append()

            if msg[0] == 'get_last_msg':
                topic_name = msg[1]
                consumer_queue = msg[2]
                consumer_recv_event = msg[3]

                if not topic_name in self.topic_list:
                    print('"{}" is not a registered topic.'.format(topic_name))
                    break

                topic_offset = self.offsets[topic_name]
                msg = self.data[topic_name][-1]  # .copy()
                consumer_queue.put(msg)  # self.offset]
                consumer_recv_event.wait()
                # print(f'Single most recent message was delivered from topic {topic_name}, offset {topic_offset}.')
                # self.consumers[topic_name].append((consumer_offset, consumer_queue))

                # new_consumer_server_thread = threading.Thread(target=self.serve_consumer, args=(topic_name, consumer_offset, consumer_queue, consumer_recv_event, notifyer_event))
                # new_consumer_server_thread.start()
                # self.consumer_servers.append()

                
                
            elif msg[0] == 'new_topic':
                topic_name = msg[1]
                n_samples = msg[2]
                print(f'New topic: {topic_name}')
                data_list = []
                for _ in range(n_samples):
                    data_list.append(None)
                self.data[topic_name] = data_list
                # self.consumers[topic_name] = []
                self.offsets[topic_name] = 0
                self.topic_lengths[topic_name] = n_samples
                self.consumer_thread_notifyers[topic_name] = {}
                self.topic_list.append(topic_name)
                self.topic_locks[topic_name] = threading.Lock()
            
            # self.init_queue.put('Hei')
            # print(new_consumer_msg)
            # break

    def serve_consumer(self, topic_name, consumer_offset, consumer_queue, consumer_recv_event, consumer_event):
        while True:
            keep_waiting = True
            while keep_waiting:
                with self.topic_locks[topic_name]:
                    topic_offset = self.offsets[topic_name]
                if consumer_offset >= topic_offset:
                    consumer_event.wait(timeout=None)
                    consumer_event.clear()
                else:
                    keep_waiting = False

            with self.topic_locks[topic_name]:
                topic_offset = self.offsets[topic_name]
                idx = consumer_offset - topic_offset + self.topic_lengths[topic_name]
                # print('idx={}, consumer offset={}, producer offset={}, n_msgs={}'.format(idx, self.offset, topic_offset, self.n_msg_topic))
                if idx >= self.topic_lengths[topic_name]:
                    # print('Error: Index out of range. idx={}, n_msgs={}, consumer offset={}, topic offset={}'.format(idx, self.topic_lengths[topic_name], self.offset, topic_offset, topic_offset))
                    msg = None
                else:
                    try:
                        msg = self.data[topic_name][idx]  # .copy()
                    except IndexError:
                        print('IndexError: Index out of range.')  #  idx={}, n_msgs={}, consumer offset={}, topic offset={}'.format(idx, self.n_msg_topic, self.offset, topic_offset, topic_offset))

            # if idx >= self.n_msg_topic:
            #     print(idx, self.offset, self.offset_dict.get(self.topic), self.n_msg_topic)
            #     msg = self.shared_dict.get(self.topic)[idx - 1]
            # else:
            # print(msg)
            
            consumer_queue.put([consumer_offset + 1, msg])  # self.offset]

            consumer_recv_event.wait()
            consumer_offset += 1

    def msg_listener(self):
        print('Start listening for messages.')
        msg_queue = self.manager.get_producer_queue()
        while True:
            # print('Waiting for messages...')
            try:
                topic, msg = msg_queue.get()
                
            except ConnectionResetError:
                print('Server closed.')
                break
            
            # self.init_queue.put('Hei')
            with self.topic_locks[topic]:
                self.data[topic].append(msg)
                self.data[topic].pop(0)
                self.offsets[topic] = self.offsets[topic] + 1
            # print(self.offsets)
            # print(self.data)
            # print(topic, msg)

            for consumer_id, event in self.consumer_thread_notifyers[topic].items():
                event.set()
            

            # for consumer_id, event in self.topic_dict.get(topic).items():
                # event.set()

        
    @staticmethod
    def start_server(ip, port):
        manager = MyManager(server=True, address=(ip, port))
        manager.start()

    def start(self):
        self.server_process.start()
        time.sleep(2)
        self.manager = MyManager(address=(self.ip, self.port))  # , authkey=b'supersecretauthkey')
        self.manager.connect()  

        self.consumer_listener_thread.start()
        self.msg_listener_thread.start()

        
