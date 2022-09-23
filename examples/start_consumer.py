from src.nqkafka.consumer import KafkaConsumer


if __name__ == '__main__':

    kafka_consumer = KafkaConsumer('time', bootstrap_servers='localhost:40000', mode='not_from_beginning')

    for msg in kafka_consumer:
        # print(msg)
        print(kafka_consumer.offset)