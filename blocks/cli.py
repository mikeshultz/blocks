from blocks.conductor.api import api, init_flask
from blocks.threads import start_thread
from blocks.enums import WorkerType


def start_conductor():
    """ Startup the conductor """
    init_flask()
    api()


def start_block_consumer():
    """ Startup the block consumer """
    start_thread(WorkerType.BLOCK)


def start_transaction_consumer():
    """ Startup the block consumer """
    start_thread(WorkerType.TRANSACTION)
