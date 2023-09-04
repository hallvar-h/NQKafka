import multiprocessing as mp
from multiprocessing.managers import SyncManager
import queue
import time


class MyManager:
    def __init__(self, server=False, authkey=b'supersecretauthkey', *args, **kwargs):
        class DictManager(SyncManager):
            pass

        if server:
            init_queue = queue.Queue()
            
            def get_init_queue():
                return init_queue

            producer_queue = queue.Queue()
            
            def get_producer_queue():
                return producer_queue

            DictManager.register('get_init_queue', callable=get_init_queue)
            DictManager.register('get_producer_queue', callable=get_producer_queue)
        else:
            DictManager.register('get_init_queue')
            DictManager.register('get_producer_queue')

        self._manager = DictManager(authkey=authkey, *args, **kwargs)
        # if 'authkey' in kwargs:
        mp.current_process().authkey = authkey

        # self.connect = self._manager.connect
        # self.get_init_queue = self._manager.connect
        [setattr(self, attr, getattr(self._manager, attr)) for attr in [
            'get_init_queue', 'get_producer_queue', 'Queue', 'Event',
        ]]

    def start(self):
        s = self._manager.get_server()
        print('Serving forever...')
        s.serve_forever()

    def connect(self, timeout=10):
        t_0 = time.time()
        while time.time() - t_0 < timeout:
            try:
                print('Trying to connect...')
                self._manager.connect()
            except:
                print('Server not found, trying again...')
                time.sleep(1)


"""
On produce:
data.append(msg)
data.pop(0)
# print(data)
offset = self.offset_dict.get(topic) + 1
self.offset_dict.update([(topic, offset)])

for consumer_id, event in self.topic_dict.get(topic).items():
# time.sleep(0.1)
event.set()
"""