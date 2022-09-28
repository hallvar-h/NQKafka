from nqkafka.producer import KafkaProducer
import numpy as np
import time
import sys


if __name__ == '__main__':

    kafka_producer = KafkaProducer(bootstrap_servers='localhost:40000')  # 'localhost:9092')

    k = 0
    while True:
        sys.stdout
        # time.sleep(1)

        payload = np.random.randn(10)
        kafka_producer.send('time', [time.time(), k, payload])
        kafka_producer.flush()
        k += 1
