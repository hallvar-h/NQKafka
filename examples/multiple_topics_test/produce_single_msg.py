from nqkafka import KafkaProducer, KafkaConsumer
import numpy as np
import time


if __name__ == '__main__':

    kafka_producer = KafkaProducer(bootstrap_servers='localhost:40000')  # 'localhost:9092')
    # kafka_producer_2 = KafkaProducer(bootstrap_servers='localhost:40000')  # 'localhost:9092')


    kafka_producer.send('topic_1', 'Hello topic 1')
    kafka_producer.send('topic_2', 'Hello topic 2')
    # kafka_producer.flush()

    # kafka_consumer = KafkaConsumer('topic_1', bootstrap_servers='localhost:40000')
    # msg_1 = next(iter(kafka_consumer))
    # print(msg_1)
    # kafka_consumer_2 = KafkaConsumer('topic_2', bootstrap_servers='localhost:40000')
    # msg_2 = next(iter(kafka_consumer_2))