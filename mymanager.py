import multiprocessing as mp
from multiprocessing.managers import SyncManager


class MyManager:
    def __init__(self, server=False, *args, **kwargs):
        class DictManager(SyncManager):
            pass

        if server:
            queue_dict = dict()
            event_dict = dict()
            def get_queue_dict():
                return queue_dict

            def get_event_dict():
                return event_dict
            SyncManager.register('get_queue_dict', callable=get_queue_dict)
            SyncManager.register('get_event_dict', callable=get_event_dict)
        else:
            DictManager.register('get_queue_dict')
            DictManager.register('get_event_dict')

        self._manager = DictManager(*args, **kwargs)
        if 'authkey' in kwargs:
            mp.current_process().authkey = kwargs['authkey']

        self.connect = self._manager.connect
        # self.get_init_queue = self._manager.connect
        [setattr(self, attr, getattr(self._manager, attr)) for attr in [
            'connect', 'get_queue_dict', 'get_event_dict'
        ]]

    def start(self):
        s = self._manager.get_server()
        print('Serving forever...')
        s.serve_forever()