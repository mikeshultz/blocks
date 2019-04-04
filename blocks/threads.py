""" Handling of starting threads """
import sys
import time
import random
import signal
from enum import Enum
from .db import LockModel, LockExists, create_initial
from .config import DSN, LOGGER
from .blocks import StoreBlocks
from .transactions import StoreTransactions

log = LOGGER.getChild('blocks')


class ThreadType(Enum):
    BLOCK = 'block-consumer'
    TRANSACTION = 'transaction-consumer'


def start_thread(thread_type):
    """ Run the consumer """

    if not isinstance(thread_type, ThreadType):
        raise ValueError("Invalid ThreadType")

    log.info("Checking database.")

    ThreadClass = None
    if thread_type == ThreadType.BLOCK:
        ThreadClass = StoreBlocks
    elif thread_type == ThreadType.TRANSACTION:
        ThreadClass = StoreTransactions
    else:
        raise Exception("Unknown thread type")

    startup = True
    main_thread = None
    lock_name = str(thread_type)
    lock = None
    pid = random.randint(0, 999)

    create_initial(DSN)

    # Model for lock management
    lockMod = LockModel(DSN)

    def shutdown(signum, frame):
        log.debug('Caught signal %d. Shutting down...' % signum)
        if main_thread:
            main_thread.shutdown.set()
            # wait for shutdown
            while main_thread.is_alive():
                continue
            lockMod.unlock(lock_name, pid)

        log.info("Clean shut down. Goodbye.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    main_thread = ThreadClass()
    main_thread.daemon = True

    while lock or startup:

        try:
            log.info("Trying to get lock '%s' for PID %s" % (lock_name, pid))
            lock = lockMod.lock(lock_name, pid)
        except LockExists as e:
            log.warning(str(e))

        # If we have a lock, but thread doesn't exist or died for some reason
        if lock and (main_thread is None or not main_thread.is_alive()):
            log.info("Starting consumer...")
            main_thread = ThreadClass()
            main_thread.daemon = True
            main_thread.start()
            startup = False

        # If main thread exists but we don't have a lock, shutdown
        elif main_thread is not None and main_thread.is_alive() and not lock:
            log.info("Lost lock, stopping consumption.")
            main_thread.shutdown.set()
            lockMod.unlock(lock_name, pid)
            startup = True

        time.sleep(15)
