""" Exceptions for the consumers """


class InvalidRange(IndexError):
    pass


class LockExists(Exception):
    pass


class ProcessShutdown(Exception):
    pass
