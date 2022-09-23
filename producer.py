from mymanager import MyManager
import time


class KafkaProducerLookalike:
    def __init__(self, bootstrap_servers, value_serializer=None):

        ip, port_str = bootstrap_servers.split(':')
        port = int(port_str)
        manager = MyManager(address=(ip, port), authkey=b'abracadabra')
        manager.connect()
        self.shared_dict = manager.get_queue_dict()
        self.topic_dict = manager.get_event_dict()

    def send(self, topic, msg):
        self.shared_dict.update([(topic, msg)])
        mark_for_deletion = []
        for consumer_id, event in self.topic_dict.get(topic).items():
            if event.is_set():
                mark_for_deletion.append(consumer_id)
            else:
                event.set()

        for id in mark_for_deletion:
            self.topic_dict.get(topic).pop(id)

        print(self.topic_dict.get(topic))

    def flush(self):
        pass


if __name__ == '__main__':
    import socket
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    kafka_producer = KafkaProducerLookalike(bootstrap_servers=bootstrap_servers)  # 'localhost:9092')

    k = 0
    while True:
        # time.sleep(0.1)
        k += 0.02

        kafka_producer.send('time', k)
        kafka_producer.flush()
