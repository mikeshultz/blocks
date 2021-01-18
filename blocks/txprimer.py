""" consumer.py is what stuffs the DB """
import os
import threading
from time import sleep
from uuid import uuid4
from time import sleep
from datetime import datetime, timedelta
from psycopg2.errors import UniqueViolation
from eth_utils import add_0x_prefix
from web3 import Web3, HTTPProvider

from blocks.config import DSN, JSONRPC_NODE, LOGGER
from blocks.db import BlockModel, TransactionModel
from blocks.enums import WorkerType
from blocks.conductorclient import ConnectionError, ping, job_request, job_submit

log = LOGGER.getChild(__name__)


class TransactionPriming(threading.Thread):
    """ Populate basic tx association data per block. This basically creates
    the link between block and transaction.  tx details are primed by another
    process. """

    def __init__(self):
        super(TransactionPriming, self).__init__()
        self.uuid = str(uuid4())
        self.block_model = BlockModel(DSN)
        self.tx_model = TransactionModel(DSN)
        self.last_ping = None

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

    def process_blocks(self):
        """ Prime transactions into the DB for blocks given in a job """

        log.debug("process_blocks...")

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
                job_response = job_request(self.uuid, WorkerType.TX_PRIME)
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

                block = self.get_block(block_no)

                log.debug("Processing block {}".format(block_no))

                for tx_hash in block['transactions']:
                    normal_hash = add_0x_prefix(tx_hash.hex())
                    try:
                        self.tx_model.query(
                            "INSERT INTO transaction (hash, dirty, block_number) "
                            "VALUES ({}, {}, {});",
                            normal_hash,
                            True,
                            block_no,
                            commit=True
                        )
                    except UniqueViolation:
                        # TODO: Should we do something more intelligent?
                        log.warning("Transaction {} exists.".format(normal_hash))
                        pass

                self.block_model.query(
                    "UPDATE BLOCK set primed = true WHERE block_number = {}",
                    block_no,
                    commit=True
                )

            job_submit(job['job_uuid'])

    def run(self):
        """ Kick off the process """

        log.info("Starting transaction consumer...")

        self.process_blocks()
