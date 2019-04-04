from .threads import ThreadType, start_thread


def start_block_consumer():
    """ Startup the block consumer """
    start_thread(ThreadType.BLOCK)


def start_transaction_consumer():
    """ Startup the block consumer """
    start_thread(ThreadType.TRANSACTION)
