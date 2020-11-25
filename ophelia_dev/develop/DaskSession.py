from dask.distributed import Client

class DaskSession:
    
    def __init__(self):
        self.client = Client()
