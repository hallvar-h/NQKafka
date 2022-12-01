from nqkafka.producer import KafkaProducer
import time


if __name__ == '__main__':

    kafka_producer = KafkaProducer(bootstrap_servers='localhost:40000')  # 'localhost:9092')

    payload = [1, 2, 3]
    kafka_producer.send('time', payload)
    kafka_producer.flush()