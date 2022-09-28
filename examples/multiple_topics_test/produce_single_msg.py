from nqkafka import KafkaProducer


if __name__ == '__main__':

    kafka_producer = KafkaProducer(bootstrap_servers='localhost:40000')

    kafka_producer.send('topic_1', 'Hello topic 1')
    kafka_producer.send('topic_2', 'Hello topic 2')