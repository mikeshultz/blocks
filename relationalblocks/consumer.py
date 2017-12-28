""" consumer.py is what stuffs the DB """
import sys
import time
import logging
import random
import threading
from datetime import datetime
from eth_utils.encoding import big_endian_to_int
from .config import DSN, JSONRPC_NODE, LOGGER
from .db import BlockModel, TransactionModel, LockModel, LockExists, create_initial
from web3 import Web3, HTTPProvider

log = LOGGER.getChild('consumer')

LOCK_NAME = 'consumer'
# Create a unique-ish pid for ourselves
PID = random.randint(0,999)


class StoreBlocks(object):
    """ Iterate through all necessary blocks and store them in the DB """

    def __init__(self):
        self.web3 = Web3(HTTPProvider(JSONRPC_NODE))
        self.latest_in_db = 0
        self.latest_on_chain = -1
        self.model = BlockModel(DSN)
        self.tx_model = TransactionModel(DSN)

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
                     fully synced.",
                     self.latest_on_chain, self.latest_in_db)
            # Bail
            return
        else:
            log.info("Processing blocks %s through %s.",
                     self.latest_in_db, self.latest_on_chain)

        block_range = range(self.latest_in_db, self.latest_on_chain)

        for block_no in block_range:

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

    def start(self):
        """ Kick off the process """
        self.get_meta()
        self.process_blocks()

def worker():
    """ Create a StoreBlocks thread """

    log.info("Starting block consumer...")

    # Look for a pre-existing lock
    lock = LockModel(DSN)
    try:
        log.info("Trying to get lock '%s' for PID %s" % (LOCK_NAME, PID))
        res = lock.lock(LOCK_NAME, PID)
        if res is True:
            log.info("Successfully got lock '%s' for PID %s" % (LOCK_NAME, PID))
            store = StoreBlocks()
            store.start()
            return True
        else:
            log.info("Unable to get lock '%s' for PID %s" % (LOCK_NAME, PID))
            return False
    except LockExists as e:
        log.warning(str(e))
        return False

def main():
    """ Run the consumer """

    log.info("Checking database.")

    create_initial(DSN)

    current_thread = None

    try:
        while True:
            if current_thread is None or not current_thread.is_alive():
                current_thread = threading.Thread(target=worker, daemon=True)
                current_thread.start()
            time.sleep(15)
    except KeyboardInterrupt:
        # TODO: stop thread
        sys.exit(0)