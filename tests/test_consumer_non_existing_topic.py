from ensurepip import bootstrap
from nqkafka import NQKafkaServer, KafkaProducer, KafkaConsumer
from nqkafka.utils import create_topic, stop_server
import multiprocessing as mp
import sys
import time


def run_server(bootstrap_servers):
    server = NQKafkaServer(bootstrap_servers)
    server.start()


def test():
    bootstrap_servers = 'localhost:40020'

    n_msgs = 20
    run_server(bootstrap_servers)

    create_topic('time', bootstrap_servers=bootstrap_servers, n_samples=50)

    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)  # 'localhost:9092')
    kafka_producer.send('time', 'Some message')
    kafka_producer.flush()
    
    kafka_consumer_err = KafkaConsumer('asd', bootstrap_servers=bootstrap_servers)

    kafka_consumer = KafkaConsumer('time', bootstrap_servers=bootstrap_servers, mode='from_beginning')
    print(next(iter(kafka_consumer)).value)
    
    stop_server(bootstrap_servers)

    sys.exit()


if __name__ == '__main__':
    test()