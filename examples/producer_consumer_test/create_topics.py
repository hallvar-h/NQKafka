from src.nqkafka.utils import create_topic


if __name__ == '__main__':

    bootstrap_servers = 'localhost:40000'
    create_topic('time', bootstrap_servers=bootstrap_servers, n_samples=50)