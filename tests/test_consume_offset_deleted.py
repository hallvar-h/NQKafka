from ensurepip import bootstrap
from nqkafka import NQKafkaServer, KafkaProducer, KafkaConsumer
from nqkafka.utils import create_topic, stop_server, consumer_seek_relative_offset
import multiprocessing as mp
import sys
import time


def run_server(bootstrap_servers):
    server = NQKafkaServer(bootstrap_servers)
    server.start()


def run_producer(bootstrap_servers, n_msgs):
    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)  # 'localhost:9092')

    k = 0
    while k < n_msgs:
        # time.sleep(0.1)

        payload = [1, 2, 3]
        kafka_producer.send('time', [time.time(), k, payload])
        kafka_producer.flush()
        k += 1

    print('\nPRODUCER: All messages sent')


if __name__ == '__main__':
    
    bootstrap_servers = 'localhost:40008'
    n_msgs = 3
    run_server(bootstrap_servers)

    create_topic('time', bootstrap_servers=bootstrap_servers, n_samples=2)
    time.sleep(2)
    kafka_consumer = KafkaConsumer('time', bootstrap_servers=bootstrap_servers, mode='from_beginning')

    run_producer(bootstrap_servers, n_msgs)
    
    # consumer_seek_relative_offset(kafka_consumer, -11)
    msg = next(iter(kafka_consumer))
    print(msg.value)