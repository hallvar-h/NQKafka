import multiprocessing as mp
from multiprocessing.managers import SyncManager


class MyManager:
    def __init__(self, server=False, *args, **kwargs):
        class DictManager(SyncManager):
            pass

        if server:
            queue_dict = dict()
            def get_queue_dict():
                return queue_dict
            SyncManager.register('get_queue_dict', callable=get_queue_dict)
        else:
            DictManager.register('get_queue_dict')

        self._manager = DictManager(*args, **kwargs)
        if 'authkey' in kwargs:
            mp.current_process().authkey = kwargs['authkey']

        self.connect = self._manager.connect
        self.get_init_queue = self._manager.connect
        [setattr(self, attr, getattr(self._manager, attr)) for attr in [
            'connect', 'get_queue_dict'
        ]]

    def start(self):
        s = self._manager.get_server()
        print('Serving forever...')
        s.serve_forever()