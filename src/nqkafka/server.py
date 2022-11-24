from .mymanager import MyManager
import multiprocessing as mp
import time
import threading

class NQKafkaServer:
    def __init__(self, bootstrap_servers):
        self.ip, port_str = bootstrap_servers.split(':')
        self.port = int(port_str)
        self.data = {}
        self.consumers = {}
        self.offsets = {}
        self.topic_lengths = {}
        
        self.server_process = mp.Process(target=self.start_server, args=(self.ip, self.port,))

        self.consumer_listener_thread = threading.Thread(target=self.consumer_listener)
        self.msg_listener_thread = threading.Thread(target=self.msg_listener)

    def consumer_listener(self):
        init_queue = self.manager.get_init_queue()
        while True:
            print('Waiting for consumers...')
            try:
                msg = init_queue.get()
            except ConnectionResetError:
                print('Server closed.')
                break
            
            if msg[0] == 'consumer':
                topic_name = msg[1]
                consumer_queue = msg[2]
                mode = msg[3]
                topic_offset = self.offsets[topic_name]
                if mode == 'from_beginning':
                    consumer_offset = max(0, topic_offset - self.topic_lengths[topic_name])
                else:
                    consumer_offset = topic_offset
                # self.consumers[topic_name].append((consumer_offset, consumer_queue))
                new_consumer_server_thread = threading.Thread(target=self.serve_consumer, args=(topic_name, consumer_offset, consumer_queue))
                self.consumer_servers.append()
                
            elif msg[0] == 'new_topic':
                topic_name = msg[1]
                n_samples = msg[2]
                print(f'New topic: {topic_name}')
                data_list = []
                for _ in range(n_samples):
                    data_list.append(None)
                self.data[topic_name] = data_list
                self.consumers[topic_name] = []
                self.offsets[topic_name] = 0
                self.topic_lengths[topic_name] = n_samples
            
            # self.init_queue.put('Hei')
            # print(new_consumer_msg)
            # break

    def serve_consumer(self, consumer_queue):
        while True:

    def msg_listener(self):
        msg_queue = self.manager.get_producer_queue()
        while True:
            # print('Waiting for messages...')
            try:
                topic, msg = msg_queue.get()
                
            except ConnectionResetError:
                print('Server closed.')
                break
            
            # self.init_queue.put('Hei')
            self.data[topic].append(msg)
            self.data[topic].pop(0)
            self.offsets[topic] = self.offsets[topic] + 1
            print(self.offsets)

            

            # # print(data)
            

            # for consumer_id, event in self.topic_dict.get(topic).items():
                # event.set()

        
    @staticmethod
    def start_server(ip, port):
        manager = MyManager(server=True, address=(ip, port))
        manager.start()

    def start(self):
        self.server_process.start()
        self.manager = MyManager(address=(self.ip, self.port))  # , authkey=b'supersecretauthkey')
        self.manager.connect()

        self.consumer_listener_thread.start()
        self.msg_listener_thread.start()

        