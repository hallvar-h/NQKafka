from src.nqkafka.server import NQKafkaServer


if __name__ == '__main__':

    bootstrap_servers = 'localhost:40000'
    server = NQKafkaServer(bootstrap_servers)
    server.start()
