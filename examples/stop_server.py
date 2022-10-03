from nqkafka.utils import stop_server

if __name__ == '__main__':
    bootstrap_servers = 'localhost:40000'
    stop_server(bootstrap_servers)