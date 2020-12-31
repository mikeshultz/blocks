""" consumer.py is what stuffs the DB """
import os
import threading
from time import sleep
from uuid import uuid4
from datetime import datetime, timedelta
from psycopg2.errors import UniqueViolation
from eth_utils.encoding import big_endian_to_int
from eth_utils.hexadecimal import encode_hex
from web3 import Web3, HTTPProvider

from blocks.config import DSN, JSONRPC_NODE, LOGGER
from blocks.db import BlockModel, TransactionModel
from blocks.enums import WorkerType
from blocks.conductorclient import ConnectionError, ping, job_request, job_submit, job_reject

log = LOGGER.getChild(__name__)


class StoreBlocks(threading.Thread):
    """ Iterate through all necessary blocks and store them in the DB """

    def __init__(self):
        super(StoreBlocks, self).__init__()

        self.uuid = str(uuid4())
        self.latest_in_db = 0
        self.latest_on_chain = -1
        self.last_ping = None

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

        log.debug("Fetching block {}".format(blk_no))

        if not isinstance(blk_no, int):
            raise ValueError("block_no must be an integer")

        return self.web3.eth.getBlock(blk_no)

    def get_meta(self):
        """ Populate some things we'll need later """

        # Set latest block no
        self.latest_on_chain = self.web3.eth.blockNumber

        res = self.model.get_latest()

        if res:
            log.debug("Latest in DB: {}".format(res))
            self.latest_in_db = res
        else:
            log.debug("Nothing in DB")
            self.latest_in_db = 0

    def process_blocks(self):
        """ Process the blocks from the chain """

        while True:

            # If we've been told to shutdown...
            if self.shutdown.is_set():
                log.info("Shutting down gracefully...")
                break

            if (
                self.last_ping is None
                or self.last_ping < datetime.now() - timedelta(seconds=15)
            ):
                try:
                    ping(self.uuid)

                except ConnectionError:
                    log.warning('Unable to connect to the conductor.')
                    sleep(3)
                    continue

            job_response = None

            try:
                job_response = job_request(self.uuid, WorkerType.BLOCK)
            except ConnectionError:
                log.error('Failed to connect to the conductor.')
                sleep(3)
                continue

            if not job_response or not job_response.get('success'):
                log.error('Invalid response from conductor')
                # TODO: Bail after a while?
                sleep(3)
                continue

            job = job_response['data']

            for block_no in job['block_numbers']:

                # If we've been told to shutdown...
                if self.shutdown.is_set():
                    log.info("Shutting down gracefully...")
                    break

                blk = self.get_block(block_no)

                try:

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
                except UniqueViolation:
                    log.warning('Block {} already exists in database'.format(block_no))
                    job_reject(job.get('job_uuid'), 'Block {} already exist in database'.format(block_no))
                    continue

                # Insert transactions
                log.debug("Block has {} transactions".format(len(blk['transactions'])))
                for txhash in blk['transactions']:
                    hex_hash = encode_hex(txhash)
                    try:
                        self.tx_model.insert_dict({
                            'hash': hex_hash,
                            'dirty': True,
                            }, commit=True)
                    except UniqueViolation:
                        log.warning('Transaction already known: {}'.format(hex_hash))
                        pass

            job_submit(job.get('job_uuid'))

    def run(self):
        """ Kick off the process """

        self.get_meta()
        self.process_blocks()
