from src.nqkafka.producer import KafkaProducer
import numpy as np
import time


if __name__ == '__main__':
    import socket
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)  # 'localhost:9092')

    k = 0
    while True:
        time.sleep(0.1)
        k += 0.1

        kafka_producer.send('time', np.random.randn(10))
        kafka_producer.flush()
