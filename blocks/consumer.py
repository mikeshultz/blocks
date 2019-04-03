""" consumer.py is what stuffs the DB """
import os
import sys
import time
import random
import signal
import threading
from datetime import datetime
from eth_utils.encoding import big_endian_to_int
from .config import DSN, JSONRPC_NODE, LOGGER
from .db import BlockModel, TransactionModel, LockModel, LockExists, create_initial
from web3 import Web3, HTTPProvider

log = LOGGER.getChild('consumer')

LOCK_NAME = 'consumer'
# Create a unique-ish pid for ourselves
PID = random.randint(0, 999)
MAIN_THREAD = None


class ProcessShutdown(Exception):
    pass


class StoreBlocks(threading.Thread):
    """ Iterate through all necessary blocks and store them in the DB """

    def __init__(self):
        super(StoreBlocks, self).__init__()

        self.latest_in_db = 0
        self.latest_on_chain = -1

        self.model = BlockModel(DSN)
        self.tx_model = TransactionModel(DSN)

        if os.environ.get('WEB3_INFURA_API_KEY'):
            from web3.auto.infura import w3 as web3
            self.web3 = web3
        else:
            self.web3 = Web3(HTTPProvider(JSONRPC_NODE))

        self.shutdown = threading.Event()

    def get_block(self, blk_no):
        """ Gets a block """

        log.debug("Fetching block %s", blk_no)

        if not isinstance(blk_no, int):
            raise ValueError("block_no must be an integer")

        return self.web3.eth.getBlock(blk_no)

    def get_meta(self):
        """ Populate some things we'll need later """

        # Set latest block no
        self.latest_on_chain = self.web3.eth.blockNumber

        res = self.model.get_latest()

        if res:
            log.debug("Latest in DB: %s", res)
            self.latest_in_db = res
        else:
            log.debug("Nothing in DB")
            self.latest_in_db = 0

    def process_blocks(self):
        """ Process the blocks from the chain """

        if self.latest_on_chain < self.latest_in_db:
            log.warning("Latest block on chain(%s) should not be less than the \
latest in DB(%s).  This could just be that the node is not \
fully synced.", self.latest_on_chain, self.latest_in_db)
            # Bail
            return
        else:
            log.info("Processing blocks %s through %s.",
                     self.latest_in_db, self.latest_on_chain)

        block_range = range(self.latest_in_db, self.latest_on_chain)

        for block_no in block_range:

            # If we've been told to shutdown...
            if self.shutdown.is_set():
                log.info("Shutting down gracefully...")
                break

            blk = self.get_block(block_no)

            self.model.insert_dict({
                'block_number': block_no,
                'block_timestamp': datetime.fromtimestamp(blk['timestamp']),
                'difficulty': blk['difficulty'],
                'hash': blk['hash'],
                'miner': blk['miner'],
                'gas_used': blk['gasUsed'],
                'gas_limit': blk['gasLimit'],
                'nonce': big_endian_to_int(blk['nonce']),
                'size': blk['size'],
                }, commit=True)

            # Insert transactions
            log.debug("Block has %s transactions", len(blk['transactions']))
            for txhash in blk['transactions']:

                tx = self.web3.eth.getTransaction(txhash)
                print(tx)
                self.tx_model.insert_dict({
                    'hash': tx['hash'],
                    'block_number': block_no,
                    'from_address': tx['from'],
                    'to_address': tx['to'],
                    'value': tx['value'],
                    'gas_price': tx['gasPrice'],
                    'gas_limit': tx['gas'],
                    'nonce': tx['nonce'],
                    'input': tx['input'],
                    }, commit=True)

    def run(self):
        """ Kick off the process """

        self.get_meta()
        self.process_blocks()


def main():
    """ Run the consumer """

    log.info("Checking database.")

    startup = True
    lock = None

    create_initial(DSN)

    # Model for lock management
    lockMod = LockModel(DSN)

    def shutdown(signum, frame):
        log.debug('Caught signal %d. Shutting down...' % signum)
        if MAIN_THREAD:
            MAIN_THREAD.shutdown.set()
            # wait for shutdown
            while MAIN_THREAD.is_alive():
                continue
            lockMod.unlock(LOCK_NAME, PID)

        log.info("Clean shut down. Goodbye.")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    MAIN_THREAD = StoreBlocks()
    MAIN_THREAD.daemon = True

    while lock or startup:

        try:
            log.info("Trying to get lock '%s' for PID %s" % (LOCK_NAME, PID))
            lock = lockMod.lock(LOCK_NAME, PID)
        except LockExists as e:
            log.warning(str(e))

        # If we have a lock, but thread doesn't exist or died for some reason
        if lock and (MAIN_THREAD is None or not MAIN_THREAD.is_alive()):
            log.info("Starting consumer...")
            MAIN_THREAD = StoreBlocks()
            MAIN_THREAD.daemon = True
            MAIN_THREAD.start()
            startup = False

        # If main thread exists but we don't have a lock, shutdown
        elif MAIN_THREAD is not None and MAIN_THREAD.is_alive() and not lock:
            log.info("Lost lock, stopping consumption.")
            MAIN_THREAD.shutdown.set()
            lockMod.unlock(LOCK_NAME, PID)
            startup = True

        time.sleep(15)
