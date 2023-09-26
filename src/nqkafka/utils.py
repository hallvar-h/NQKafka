from .mymanager import MyManager
from multiprocessing.managers import SyncManager
from multiprocessing.managers import dispatch, listener_client


def consumer_seek_relative_offset(consumer, relative_offset):
    consumer.init_queue.put(['consumer_seek_relative_offset', consumer.id, consumer.topic, relative_offset])
    # consumer.offset += relative_offset


def create_topic(name, bootstrap_servers, n_samples=10):

    ip, port_str = bootstrap_servers.split(':')
    port = int(port_str)
    manager = MyManager(address=(ip, port))
    manager.connect()

    init_queue = manager.get_init_queue()
    init_queue.put(['new_topic', name, n_samples])


def stop_server(bootstrap_servers, authkey=b'supersecretauthkey'):
    ip, port_str = bootstrap_servers.split(':')
    port = int(port_str)

    # From stackoverflow:
    # https://stackoverflow.com/questions/44940164/stopping-a-python-multiprocessing-basemanager-serve-forever-server
    _Client = listener_client['pickle'][1]
    conn = _Client(address=(ip, port), authkey=authkey)
    dispatch(conn, None, 'shutdown')
    conn.close()
    print('NQKafka server closed.')


def get_last_message_from_topic(bootstrap_servers, topic):
    ip, port_str = bootstrap_servers.split(':')
    port = int(port_str)
    manager = MyManager(address=(ip, port))  # , authkey=b'supersecretauthkey')
    manager.connect()

    init_queue = manager.get_init_queue()
    input_stream = manager.Queue()
    recv_event = manager.Event()
    recv_event.clear()
    init_queue.put(['get_last_msg', topic, input_stream, recv_event])
    
    msg = input_stream.get()
    recv_event.set()
    kafka_msg = type('KafkaMsg', (), {'value': msg})
    return kafka_msg.value  # self.offset]
    


if __name__ == '__main__':
    ip = 'localhost'  # socket.gethostbyname(socket.gethostname())
    port = 40000
    bootstrap_servers = ip + ':' + str(port)

    create_topic('some_topic', bootstrap_servers=bootstrap_servers, n_samples=50)