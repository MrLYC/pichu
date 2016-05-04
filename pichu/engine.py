from contextlib import closing


class BaseEngine(object):

    def __init__(self):
        super(BaseEngine, self).__init__()

    def get_cursor(self, *args, **kwargs):
        raise NotImplemented
