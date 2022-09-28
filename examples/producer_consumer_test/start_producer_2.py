from nqkafka.producer import KafkaProducer
import numpy as np
import time


if __name__ == '__main__':

    kafka_producer = KafkaProducer(bootstrap_servers='localhost:40000')  # 'localhost:9092')

    k = 0
    while True:
        time.sleep(1)
        k += 0.1

        payload = [1, 2, 3]
        kafka_producer.send('time', payload)
        kafka_producer.flush()