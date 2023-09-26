from ensurepip import bootstrap
from nqkafka import NQKafkaServer, KafkaProducer, KafkaConsumer
from nqkafka.utils import create_topic, stop_server
import multiprocessing as mp
import sys
import time


def run_server(bootstrap_servers):
    server = NQKafkaServer(bootstrap_servers)
    server.start()


def run_consumer(bootstrap_servers, n_msgs):
    kafka_consumer = KafkaConsumer('time', bootstrap_servers=bootstrap_servers, mode='from_beginning')
    for msg in kafka_consumer:
        print(msg)
    # msg_gen = iter(kafka_consumer)
    # msg = next(msg_gen)


def run_producer(bootstrap_servers, n_msgs):
    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)  # 'localhost:9092')

    k = 0
    while k < n_msgs:
        time.sleep(0.1)

        payload = [1, 2, 3]
        kafka_producer.send('time', [time.time(), k, payload])
        kafka_producer.flush()
        k += 1

    print('\nPRODUCER: All messages sent')




def test():
    bootstrap_servers = 'localhost:40001'

    n_msgs = 20
    run_server(bootstrap_servers)

    create_topic('time', bootstrap_servers=bootstrap_servers, n_samples=50)
    # time.sleep(2)

    p_consumer = mp.Process(target=run_consumer, args=(bootstrap_servers, n_msgs,))
    p_consumer.start()
    time.sleep(2)

    stop_server(bootstrap_servers)

    # sys.exit()


if __name__ == '__main__':
    test()