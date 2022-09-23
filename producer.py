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
    print(shared_dict)

    k = 0
    while True:
        time.sleep(0.1)
        k += 0.1
        shared_dict.update([('time', k)])