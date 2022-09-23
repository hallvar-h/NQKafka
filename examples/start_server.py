from src.nqkafka.mymanager import MyManager


if __name__ == '__main__':
    import socket
    bootstrap_servers = 'localhost:40000'
    ip, port_str = bootstrap_servers.split(':')
    port = int(port_str)

    # topics = ['time', 'pmudata']
    manager = MyManager(server=True, address=(ip, port))
    manager.start()