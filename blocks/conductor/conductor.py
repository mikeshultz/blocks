""" consumer.py is what stuffs the DB """
import json
from math import floor
from uuid import uuid4
from web3 import Web3, HTTPProvider

from typing import Union, Optional, List, Tuple

from blocks.config import DSN, JSONRPC_NODE, LOGGER
from blocks.utils import del_listf
from blocks.db import ConsumerModel, BlockModel, TransactionModel
from blocks.enums import WorkerType

# TODO: Make bigger batch sizes, reduce request load on conductor
DEFAULT_BATCH_SIZE = 500

# Batch size divisor to be used for transaction batches.
# BATCH_SIZE / DIVISOR = TX_BATCH_SIZE
TX_BATCH_DIVISOR = 100

# The max amount of block numbers to load from the DB per single query
LOAD_BATCH_SIZE = 1000000

log = LOGGER.getChild(__name__)


class JSONSerialized:
    def to_json(self):
        return json.dumps(self.to_dict())


class BlockJob(JSONSerialized):
    def __init__(self, consumer_uuid, block_numbers):
        self.job_uuid = str(uuid4())
        self.consumer_uuid = consumer_uuid
        self.block_numbers = block_numbers

    def to_dict(self):
        return {
            'job_uuid': str(self.job_uuid),
            'consumer_uuid': str(self.consumer_uuid),
            'block_numbers': self.block_numbers,
        }


class TransactionPrimingJob(JSONSerialized):
    def __init__(self, consumer_uuid, block_numbers):
        self.job_uuid = str(uuid4())
        self.consumer_uuid = consumer_uuid
        self.block_numbers = block_numbers

    def to_dict(self):
        return {
            'job_uuid': str(self.job_uuid),
            'consumer_uuid': str(self.consumer_uuid),
            'block_numbers': self.block_numbers,
        }


class TransactionDetailJob(JSONSerialized):
    def __init__(self, consumer_uuid, transactions):
        self.job_uuid = str(uuid4())
        self.consumer_uuid = consumer_uuid
        self.transactions = transactions

    def to_dict(self):
        return {
            'job_uuid': str(self.job_uuid),
            'consumer_uuid': str(self.consumer_uuid),
            'transactions': self.transactions,
        }


JobType = Union[BlockJob, TransactionPrimingJob, TransactionDetailJob]


class Conductor:
    """ Partition out the workload and provide jobs to workers """

    def __init__(self, batch_size=None):
        self.status = False
        self.batch_size = batch_size or DEFAULT_BATCH_SIZE
        self.latest_in_db = 0
        self.latest_on_chain = -1
        self.known_block_numbers = set()
        self.selected_block_numbers = set()
        self.known_transactions = set()
        self.selected_transactions = set()
        # self.known_primed_blocks = set()
        self.selected_blocks_to_prime = set()
        self.jobs = []

        self.consumer_model = ConsumerModel(DSN)
        self.block_model = BlockModel(DSN)
        self.tx_model = TransactionModel(DSN)

        self.web3 = Web3(HTTPProvider(JSONRPC_NODE))

        self.get_meta()

        self.status = True

    def _process_block_meta(self, block_meta):
        if not block_meta or len(block_meta) < 1:
            return

        log.info('Processing block numbers from DB...')

        blocknums = [b[0] for b in block_meta]
        blocknums_primed = [b[0] for b in block_meta if b[1]]

        self.known_block_numbers.update(blocknums)
        # self.known_primed_blocks.update(blocknums_primed)

        loaded = len(blocknums)
        loaded_primed = len(blocknums_primed)

        log.debug('Loaded {} block numbers ({} primed)'.format(loaded, loaded_primed))

        return loaded > 0

    def get_meta(self):
        """ Populate some things we'll need later """
        print('get_meta|get_meta|get_meta|get_meta|get_meta')
        res = self.block_model.get_latest()

        if res:
            log.debug("Latest in DB: %s", res)

            self.latest_in_db = res
            self.latest_on_chain = self.web3.eth.blockNumber

            log.debug("Latest on chain: %s", self.latest_on_chain)

            batches = int(self.latest_on_chain / LOAD_BATCH_SIZE) + 1

            for i in range(1, batches + 1):
                end = i * LOAD_BATCH_SIZE
                start = end - LOAD_BATCH_SIZE

                log.debug('Loading blocks {}-{}'.format(start, end))

                block_meta = self.block_model.get_block_meta(
                    start=start,
                    end=end,
                )

                if not self._process_block_meta(block_meta):
                    break

        else:
            log.debug("Nothing in DB")
            self.latest_in_db = 0

    def add_consumer(self, type, name, host, port):
        """ Add a consumer to track """

        uuid = str(uuid4())

        self.consumer_model.insert_dict({
            'consumer_uuid': uuid,
            'port': port,
            'address': host,
            'name': name,
            'active': True
        })

        return uuid

    def ping(self, uuid):
        assert uuid is not None
        self.consumer_model.ping(uuid)

    def remove_consumer(self, uuid):
        self.consumer_model.deactivate(uuid)

    def get_job(self, uuid):
        """ Get an existing job for a client """
        return next(
            filter(
                lambda job: job.consumer_uuid == uuid or job.job_uuid == uuid,
                self.jobs
            ),
            None
        )

    def del_job(self, uuid):
        """ Get an existing job for a client """
        del_listf(
            self.jobs,
            lambda job: job.consumer_uuid == uuid or job.job_uuid == uuid,
        )

    def generate_job(self, worker_type: WorkerType,
                     uuid: str) -> Optional[JobType]:
        """ Figure out what needs doing and grab a chunk """

        log.info('Generating job for {} worker {}'.format(worker_type, uuid))

        existing_job = self.get_job(uuid)

        if existing_job:
            return existing_job

        job: Optional[JobType]

        if worker_type == WorkerType.BLOCK:
            job = BlockJob(consumer_uuid=uuid, block_numbers=[])

            for i in range(0, self.latest_on_chain):
                if (
                    i not in self.known_block_numbers
                    and i not in self.selected_block_numbers
                ):
                    job.block_numbers.append(i)

                if len(job.block_numbers) >= self.batch_size:
                    break

            if len(job.block_numbers) > 0:
                self.selected_block_numbers.update(job.block_numbers)
            else:
                # TODO: Don't make this request pointless.
                self.latest_on_chain = self.web3.eth.blockNumber

                log.warning(
                    'No blocks available to add to job.  Updating to block '
                    '{}'.format(
                        self.latest_on_chain
                    )
                )

        elif worker_type == WorkerType.TX_PRIME:
            job = TransactionPrimingJob(consumer_uuid=uuid, block_numbers=[])

            block_numbers = self.block_model.get_unprimed_blocks(
                limit=floor(self.batch_size / TX_BATCH_DIVISOR),
                exclude=self.selected_blocks_to_prime,
            )

            job.block_numbers = block_numbers

            self.selected_blocks_to_prime.update(job.block_numbers)

        elif worker_type == WorkerType.TX_DETAIL:
            job = TransactionDetailJob(consumer_uuid=uuid, transactions=[])

            transaction_pool = self.tx_model.get_random_dirty(
                limit=self.batch_size * 2
            )

            job.transactions = [
                tx.to_dict().get('hash')
                for tx in transaction_pool
                if (
                    tx not in self.known_transactions
                    and tx not in self.selected_transactions
                )
            ]

            self.selected_transactions.update(job.transactions)

        else:
            log.warning('Unknown worker type')
            return None

        self.jobs.append(job)

        return job

    def verify_job(self, job_uuid) -> Tuple[bool, List[str]]:
        """ Verify that a job has bee completed """
        job = self.get_job(job_uuid)

        if not job:
            return (False, ["Invalid job UUID"])

        if isinstance(job, BlockJob):
            log.debug('Verifying block job...')

            for block_number in job.block_numbers:
                valid, errors = self.block_model.validate_block(block_number)
                if valid is not True:
                    log.warning('verify of block {} failed'.format(block_number))
                    return (valid, errors)

        elif isinstance(job, TransactionPrimingJob):
            log.debug('Verifying transcation priming job...')

            if not job.block_numbers:
                log.error('Job missing block numbers, probably an error')
                return (False, ["Job missing block numbers"])

            for block_no in job.block_numbers:
                valid, errors = self.block_model.validate_block_primed(
                    block_no
                )

                if valid is not True:
                    return (valid, errors)

            # So our exlusion list doesn't grow infinitely, move blocks from
            # selected to known.
            # self.known_primed_blocks.update(job.block_numbers)
            self.selected_blocks_to_prime.difference_update(job.block_numbers)

        elif isinstance(job, TransactionDetailJob):
            log.debug('Verifying transcation job...')

            if not job.transactions:
                log.error('Job missing transactions, probably an error')
                return (False, ["Job missing transactions"])

            for tx_hash in job.transactions:
                valid, errors = self.tx_model.validate_transaction(tx_hash)

                if valid is not True:
                    return (valid, errors)

        else:
            log.warning('Unknown job type')
            return (False, ["Unknown job type"])

        self.del_job(job_uuid)

        log.debug('Verification succeeded!')

        return (True, [])
