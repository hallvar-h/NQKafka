from src.nqkafka import KafkaConsumer


if __name__ == '__main__':

    kafka_consumer = KafkaConsumer('topic_2', bootstrap_servers='localhost:40000', mode='not_from_beginning')

    msg = next(iter(kafka_consumer))
    print(kafka_consumer.offset, msg.value)