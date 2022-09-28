from nqkafka import KafkaConsumer


if __name__ == '__main__':

    kafka_consumer = KafkaConsumer('topic_1', bootstrap_servers='localhost:40000', mode='not_from_beginning')

    for msg in kafka_consumer:
        print('Received message: ', kafka_consumer.offset, msg.value)
        # pass
        # print(kafka_consumer.offset, msg)