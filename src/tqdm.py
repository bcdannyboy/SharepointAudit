# Mock tqdm module for testing

class tqdm:
    """Mock tqdm progress bar."""
    def __init__(self, *args, **kwargs):
        self.total = kwargs.get('total', 0)
        self.desc = kwargs.get('desc', '')
        self.n = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def update(self, n=1):
        self.n += n

    def close(self):
        pass

    def __iter__(self):
        return iter([])
