from .mymanager import MyManager
from multiprocessing.managers import SyncManager


def create_topic(name, bootstrap_servers, n_samples=10):

    ip, port_str = bootstrap_servers.split(':')
    port = int(port_str)
    manager = MyManager(address=(ip, port))
    manager.connect()

    data_dict = manager.get_queue_dict()
    topic_dict = manager.get_event_dict()
    offset_dict = manager.get_offset_dict()
    lock_dict = manager.get_lock_dict()

    consumer_dict = SyncManager.dict(manager._manager)
    data_list = SyncManager.list(manager._manager)
    topic_lock = SyncManager.Lock(manager._manager)
    for _ in range(n_samples):
        data_list.append(None)

    topic_dict.update([(name, consumer_dict)])
    data_dict.update([(name, data_list)])
    offset_dict.update([(name, 0)])
    lock_dict.update([(name, topic_lock)])


if __name__ == '__main__':
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    create_topic('some_topic', bootstrap_servers=bootstrap_servers, n_samples=50)