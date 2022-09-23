from src.nqkafka.utils import create_topic


if __name__ == '__main__':
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    create_topic('time', bootstrap_servers=bootstrap_servers, n_samples=50)