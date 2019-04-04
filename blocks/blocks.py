""" consumer.py is what stuffs the DB """
import os
import random
import threading
from datetime import datetime
from eth_utils.encoding import big_endian_to_int
from eth_utils.hexadecimal import encode_hex
from .config import DSN, JSONRPC_NODE, LOGGER
from .db import BlockModel, TransactionModel
from web3 import Web3, HTTPProvider

log = LOGGER.getChild(__name__)


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
                'hash': encode_hex(blk['hash']),
                'miner': blk['miner'],
                'gas_used': blk['gasUsed'],
                'gas_limit': blk['gasLimit'],
                'nonce': big_endian_to_int(blk['nonce']),
                'size': blk['size'],
                }, commit=True)

            # Insert transactions
            log.debug("Block has %s transactions", len(blk['transactions']))
            for txhash in blk['transactions']:

                self.tx_model.insert_dict({
                    'hash': encode_hex(txhash),
                    'dirty': True,
                    }, commit=True)

    def run(self):
        """ Kick off the process """

        self.get_meta()
        self.process_blocks()
