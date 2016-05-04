from functools import wraps


def chaining_method(method):
    @wraps(method)
    def method_wrapper(self, *args, **kwargs):
        method(self, *args, **kwargs)
        return self
    return method_wrapper
