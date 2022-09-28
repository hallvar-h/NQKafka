from nqkafka import KafkaConsumer


if __name__ == '__main__':

    kafka_consumer = KafkaConsumer('time', bootstrap_servers='localhost:40000', mode='from_beginning')

    for msg in kafka_consumer:
        print(kafka_consumer.offset, msg.value)
        # print(kafka_consumer.offset)