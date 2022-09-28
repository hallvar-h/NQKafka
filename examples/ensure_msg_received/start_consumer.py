from src.nqkafka import KafkaConsumer
import sys
import time


if __name__ == '__main__':

    kafka_consumer = KafkaConsumer('time', bootstrap_servers='localhost:40000', mode='from_beginning')

    k = 0
    for msg in kafka_consumer:
        t_send, k_prod, payload = msg.value
        if not k == k_prod:
            print('Wrong message received!')
        else:
            sys.stdout.write('\rCorrect message received (#{}). Delay: {:.1f} ms.'.format(k, 1e3*(time.time() - t_send)))
        k += 1
        # print(kafka_consumer.offset)