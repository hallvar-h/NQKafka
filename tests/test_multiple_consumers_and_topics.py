from ensurepip import bootstrap
from nqkafka import NQKafkaServer, KafkaProducer, KafkaConsumer
from nqkafka.utils import create_topic, stop_server
import multiprocessing as mp
import sys
import time


def run_server(bootstrap_servers):
    server = NQKafkaServer(bootstrap_servers)
    server.start()


def run_consumer(consumer_no, topic, bootstrap_servers, n_msgs):
    kafka_consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, mode='from_beginning')

    k = 0
    msg_gen = iter(kafka_consumer)
    while k < n_msgs:
        msg = next(msg_gen)
        t_send, k_prod, payload = msg.value
        if not k == k_prod:
            print(f'\nCONSUMER[{topic}]-{consumer_no}: Wrong message received!')
        # else:
            # print('Consumer{} Correct message received (#{}). Delay: {:.1f} ms.'.format(consumer_no, k, 1e3*(time.time() - t_send)))
        k += 1

    # print(f'\nCONSUMER{consumer_no}: All messages received')


def run_producer(topic, bootstrap_servers, n_msgs):
    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)  # 'localhost:9092')

    k = 0
    while k < n_msgs:
        time.sleep(0.02)

        payload = [1, 2, 3]
        kafka_producer.send(topic, [time.time(), k, payload])
        kafka_producer.flush()
        k += 1

    # print('\nPRODUCER: All messages sent')




if __name__ == '__main__':
    bootstrap_servers = 'localhost:40001'

    n_msgs = 50
    n_topics = 10
    n_consumers = 3

    run_server(bootstrap_servers)

   
    topics = [f'topic {i}' for i in range(n_topics)]
    for topic in topics:
        create_topic(topic, bootstrap_servers=bootstrap_servers, n_samples=50)

    p_consumers = {}
    p_producers = {}
    

    time.sleep(1)

    for topic in topics:
        p_consumers[topic] = []
        p_producers[topic] = []
        for consumer_no in range(n_consumers):
            p_consumer = mp.Process(target=run_consumer, args=(consumer_no, topic, bootstrap_servers, n_msgs,))
            p_consumer.start()
            p_consumers[topic].append(p_consumer)

        p_producer = mp.Process(target=run_producer, args=(topic, bootstrap_servers, n_msgs,))
        p_producer.start()
        p_producers[topic].append(p_producer)
    
    
    for topic in topics:
        print('Joining consumers')
        for p_consumer in p_consumers[topic]:
            p_consumer.join()
        print('Joining producers')
        for p_producer in p_producers[topic]:
            p_producer.join()

    print('Closing server')
    stop_server(bootstrap_servers)
    print('Server successfully closed.')

    sys.exit()