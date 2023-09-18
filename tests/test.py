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

    k = 0
    msg_gen = iter(kafka_consumer)
    while k < n_msgs:
        msg = next(msg_gen)
        t_send, k_prod, payload = msg.value
        if not k == k_prod:
            print('Wrong message received!')
        else:
            sys.stdout.write('\rCorrect message received (#{}). Delay: {:.1f} ms.'.format(k, 1e3*(time.time() - t_send)))
        k += 1

    print('\nCONSUMER: All messages received')


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
    bootstrap_servers = 'localhost:40007'

    n_msgs = 20
    run_server(bootstrap_servers)

    create_topic('time', bootstrap_servers=bootstrap_servers, n_samples=50)
    time.sleep(2)

    p_consumer = mp.Process(target=run_consumer, args=(bootstrap_servers, n_msgs,))
    p_consumer.start()

    p_producer = mp.Process(target=run_producer, args=(bootstrap_servers, n_msgs,))
    p_producer.start()

    p_consumer.join()
    p_producer.join()

    stop_server(bootstrap_servers)

    sys.exit()


if __name__ == '__main__':
    test()