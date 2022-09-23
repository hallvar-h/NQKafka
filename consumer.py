from mymanager import MyManager
import time


if __name__ == '__main__':
    import socket

    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    port = 40000
    qm_kwargs = dict(address=(ip, port), authkey=b'abracadabra')

    manager = MyManager(**qm_kwargs)

    manager.connect()

    shared_dict = manager.get_queue_dict()

    while True:
        time.sleep(0.1)
        print(shared_dict)