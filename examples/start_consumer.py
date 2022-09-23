from src.nqkafka.consumer import KafkaConsumer


if __name__ == '__main__':
    import socket
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    kafka_consumer = KafkaConsumer('time', bootstrap_servers=bootstrap_servers, mode='not_from_beginning')

    for msg in kafka_consumer:
        # print(msg)
        print(kafka_consumer.offset)