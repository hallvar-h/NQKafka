from mymanager import MyManager
from multiprocessing.managers import SyncManager


def create_topic(topic, bootstrap_servers):

    ip, port_str = bootstrap_servers.split(':')
    port = int(port_str)
    manager = MyManager(address=(ip, port), authkey=b'abracadabra')
    manager.connect()

    topic_dict = manager.get_event_dict()

    dict = SyncManager.dict(manager._manager)
    topic_dict.update([(topic, dict)])


if __name__ == '__main__':
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    create_topic('time', bootstrap_servers=bootstrap_servers)