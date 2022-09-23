from src.nqkafka.mymanager import MyManager


# KafkaConsumerLookalike()


if __name__ == '__main__':
    import socket

    hostname = socket.gethostname()
    ip = 'localhost'  # socket.gethostbyname(hostname)
    port = 40000
    qm_kwargs = dict(address=(ip, port), authkey=b'abracadabra')

    # topics = ['time', 'pmudata']
    manager = MyManager(server=True, **qm_kwargs)

    manager.start()