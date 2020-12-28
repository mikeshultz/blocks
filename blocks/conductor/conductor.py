""" consumer.py is what stuffs the DB """
import json
from uuid import uuid4
from web3 import Web3, HTTPProvider

from typing import Union, Optional, List, Tuple

from blocks.config import DSN, JSONRPC_NODE, LOGGER
from blocks.utils import del_listf
from blocks.db import ConsumerModel, BlockModel, TransactionModel
from blocks.enums import WorkerType

DEFAULT_BATCH_SIZE = 50

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


class TransactionJob(JSONSerialized):
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


JobType = Union[BlockJob, TransactionJob]


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
        self.jobs = []

        self.consumer_model = ConsumerModel(DSN)
        self.block_model = BlockModel(DSN)
        self.tx_model = TransactionModel(DSN)

        self.web3 = Web3(HTTPProvider(JSONRPC_NODE))

        self.get_meta()

        self.status = True

    def _process_block_nums(self, blocknums):
        if not blocknums or len(blocknums) < 1:
            return

        self.known_block_numbers = set(blocknums)

        log.debug('Loaded {} block numbers'.format(
            len(self.known_block_numbers)
        ))

    def get_meta(self):
        """ Populate some things we'll need later """

        res = self.block_model.get_latest()

        if res:
            log.debug("Latest in DB: %s", res)
            self.latest_in_db = res
            self.latest_on_chain = self.web3.eth.blockNumber

            self._process_block_nums(
                self.block_model.get_all_block_numbers()
            )
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

            self.selected_block_numbers.update(job.block_numbers)

        elif worker_type == WorkerType.TRANSACTION:
            job = TransactionJob(consumer_uuid=uuid, transactions=[])

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
            log.debug('Verifying transcation job...')

            for block_number in job.block_numbers:
                valid, errors = self.block_model.validate_block(block_number)
                if valid is not True:
                    log.warning('verify of block {} failed'.format(block_number))
                    return (valid, errors)

        elif isinstance(job, TransactionJob):
            log.debug('Verifying transcation job...')

            if not job.transactions:
                log.error('Job missing transactions, probably an error')
                return (False, ["Job missing transactions"])

            for tx_hash in job.transactions:
                return self.tx_model.validate_transaction(tx_hash)

        else:
            log.warning('Unknown job type')
            return (False, ["Unknown job type"])

        self.del_job(job_uuid)

        log.debug('Verification succeeded!')

        return (True, [])
