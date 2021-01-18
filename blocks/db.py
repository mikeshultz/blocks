""" Database models and utilities """
import os
import sys
import random
import psycopg2
from datetime import datetime
from eth_utils.address import is_address
from rawl import RawlBase

from typing import List, Tuple

from blocks.utils import is_256bit_hash, validate_conditions
from blocks.config import LOGGER
from blocks.exceptions import InvalidRange, LockExists

log = LOGGER.getChild('db')

MAX_LOCKS = 50


class ConsumerModel(RawlBase):
    def __init__(self, dsn: str):
        super(ConsumerModel, self).__init__(
            dsn,
            table_name='consumer',
            columns=['consumer_uuid', 'port', 'address', 'name', 'active'
                     'last_seen'],
            pk_name='consumer_uuid'
        )

    def deactivate(self, uuid):
        return self.query(
            "UPDATE consumer SET active = false WHERE consumer_uuid = {};",
            uuid, commit=True)

    def ping(self, uuid):
        return self.query(
            "UPDATE consumer SET last_seen = now() WHERE consumer_uuid = {};",
            uuid, commit=True)


class JobModel(RawlBase):
    def __init__(self, dsn: str):
        super(JobModel, self).__init__(
            dsn,
            table_name='job',
            columns=['job_id', 'blocks', 'trasnactions', 'block_range'],
            pk_name='job_id'
        )


class BlockModel(RawlBase):
    def __init__(self, dsn: str):
        super(BlockModel, self).__init__(
            dsn,
            table_name='block',
            columns=['block_number', 'block_timestamp', 'difficulty', 'hash',
                     'miner', 'gas_used', 'gas_limit', 'nonce', 'size',
                     'primed'],
            pk_name='block_number'
        )

    def count(self):
        return self.query("SELECT COUNT(*) FROM block;")[0][0]

    def get_range(self, start: datetime, end: datetime) -> tuple:
        """ Get a range of blocks from start to end """

        if start > end:
            raise InvalidRange("start must come before end")

        result = self.query(
            "SELECT MIN(block_number), MAX(block_number) FROM block"
            " WHERE block_time BETWEEN {} AND {}",
            start, end)

        return (result[0][0], result[0][1])

    def get_latest(self) -> int:
        """ Get the latest block in the DB """

        res = self.query("SELECT MAX(block_number) FROM block;")
        if res != []:
            return res[0][0]
        else:
            return 0

    def get_all_block_numbers(self) -> List[int]:
        """ Get all block numbers in the DB """

        res = self.query("SELECT block_number FROM block;")
        if res:
            return [x[0] for x in res]
        else:
            return []

    def get_block_numbers(self, start=0, end=1000000) -> List[int]:
        """ Get block numbers from the DB within the given range """

        res = self.query(
            "SELECT block_number FROM block "
            "WHERE block_number >= {} and block_number < {};",
            start,
            end
        )

        if res:
            return [x[0] for x in res]
        else:
            return []

    def get_blocks(self, start=0, end=1000000) -> List[int]:
        """ Get blocks from the DB within the given range """

        return self.select(
            "SELECT {} FROM block "
            "WHERE block_number >= {} and block_number < {};",
            self.columns, start, end)

    def get_unprimed_blocks(self, limit=50, exclude=[]) -> List[int]:
        """ Get blocks that are unprimed """
        exclusion = ""

        if len(exclude) > 0:
            exclusion = "AND block_number NOT IN ({}) ".format(
                ",".join(map(str, exclude))
            )

        return [
            x[0]
            for x in self.query(
                "SELECT block_number FROM block "
                "WHERE primed = false "
                + exclusion +
                "ORDER BY block_number DESC "
                "LIMIT {};",
                limit
            )
        ]

    def validate_block_primed(self, block_number) -> Tuple[bool, List[str]]:
        """ Validate that a block has been marked as primed """
        blocks = self.select(
            "SELECT {} FROM block WHERE block_number = {};",
            ['block_number', 'primed'], block_number)

        validated = blocks[0][1]
        errors = ['Not marked primed'] if not validated else []

        return validated, errors

    def validate_block(self, block_number) -> Tuple[bool, List[str]]:
        """ Validate that a block number exists and that its values generally
        look correct.
        """
        blocks = self.select(
            "SELECT {} FROM block WHERE block_number = {};",
            self.columns, block_number)

        count = len(blocks)

        if count < 1:
            return (False, ["No block"])

        elif count > 1:
            raise ValueError('Invalid result, duplicate blocks')

        block = blocks[0]

        return validate_conditions([
            (block.block_timestamp is not None, "block_timestamp is missing"),
            (block.difficulty is not None, "difficulty missing"),
            (block.hash is not None, "block hash missing"),
            (is_256bit_hash(block.hash), "block hash is not a hash"),
            (block.miner is not None, "miner missing"),
            (is_address(block.miner), "miner is not an address"),
            (block.gas_used is not None, "gas_used missing"),
            (block.gas_limit is not None, "gas_limit missing"),
            (block.nonce is not None, "nonce missing"),
            (block.size is not None, "size missing"),
        ])


class TransactionModel(RawlBase):
    def __init__(self, dsn: str):
        super(TransactionModel, self).__init__(
            dsn,
            table_name='transaction',
            columns=['hash', 'dirty', 'block_number', 'from_address',
                     'to_address', 'value', 'gas_price', 'gas_limit', 'nonce',
                     'input'],
            pk_name='hash'
        )

    def count(self):
        return self.query("SELECT COUNT(*) FROM transaction;")[0][0]

    def get_random_dirty(self, limit: int = 1) -> list:
        """ Get a single dirty transaction """

        return self.select(
            "SELECT {} FROM transaction"
            " WHERE dirty = true"
            " ORDER BY random() LIMIT {};",
            ['hash'], limit)

    def get_by_address(self, address: str) -> list:
        """ Get a list of transactions for an address """

        if not is_address(address):
            raise ValueError("Address is invalid")

        result = self.select(
            "SELECT {} FROM transaction"
            " WHERE from_address = {} OR to_address = {};",
            self.columns, address, address)

        return result

    def get_count(self) -> int:
        """ Get the full count of transactions """

        res = self.query("SELECT COUNT(hash) FROM transaction;")
        if res is not None:
            return res[0][0]
        else:
            return 0

    def validate_transaction(self, tx_hash) -> Tuple[bool, List[str]]:
        """ Validate that a transactions exists and that its values generally
        look correct.
        """
        transactions = self.select(
            "SELECT {} FROM transaction WHERE hash = {};",
            self.columns, tx_hash)

        count = len(transactions)

        if count < 1:
            return (False, ["No transaction"])

        elif count > 1:
            raise ValueError('Invalid result, duplicate transactions')

        tx = transactions[0]

        return validate_conditions([
            (is_256bit_hash(tx.hash), "Transaction hash is invalid"),
            (tx.dirty is False, "Transaction is marked dirty"),
            (tx.block_number is not None, "block_number missing"),
            (is_address(tx.from_address), "from_address is not an address"),
            (is_address(tx.to_address), "to_address is not an address"),
            (tx.value is not None, "value missing"),
            (tx.gas_price is not None, "gas_price missing"),
            (tx.gas_limit is not None, "gas_limit missing"),
            (tx.nonce is not None, "nonce missing"),
            (tx.input is not None, "input missing"),
        ])


class LockModel(RawlBase):
    """ Model representing a lock in the DB """

    def __init__(self, dsn: str):
        super(LockModel, self).__init__(
            dsn,
            table_name='lock',
            columns=['lock_id', 'name', 'updated', 'pid'],
            pk_name='lock_id'
        )

        self.lock_id = None
        self.name = None
        self.updated = None
        self.pid = None

    def check_lock(self, name):
        return self.select(
            "SELECT {} FROM lock WHERE name = {}"
            " AND updated + interval '1 hour' > now();",
            self.columns, name)

    def get_lock(self, lock_id):
        return self.select(
            "SELECT {} FROM lock WHERE lock_id = {};",
            self.columns, lock_id)

    def get_lock_by_pid(self, pid):
        return self.select(
            "SELECT {} FROM lock WHERE pid = {};",
            self.columns, pid)

    def get_active_lock_by_pid(self, pid):
        return self.select(
            "SELECT {} FROM lock WHERE pid = {}"
            " AND updated + interval '1 hour' > now();",
            self.columns, pid)

    def update_lock(self, lock_id):
        return self.query(
            "UPDATE lock SET updated = now() WHERE lock_id = {};",
            lock_id, commit=True)

    def add_lock(self, name, pid=random.randint(0, 999)):
        return self.insert_dict({
            "name": name,
            "pid": pid,
            }, commit=True)

    def lock(self, name, pid=random.randint(0, 999)):
        res = self.get_active_lock_by_pid(pid)

        if len(res) == 1:
            return True

        res = self.check_lock(name)

        if len(res) >= MAX_LOCKS:
            if res[0].pid != pid:
                raise LockExists("Maximum locks reached")
            return True

        else:
            self.lock_id = self.add_lock(name, pid)
            if self.lock_id:
                log.debug("Created lock #%s" % self.lock_id)
                self.name = name
                return True
            else:
                log.warn("Failed to create new lock!")
                log.debug("Expected lock_id, received: {}".format(self.lock_id))
                return False

    def unlock(self, name, pid):
        return self.query("DELETE FROM lock WHERE name = {} AND pid = {};",
                          name, pid, commit=True)


def create_initial(DSN: str) -> bool:
    """ If necessary, runs the DDL necessary for the app to function """

    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # Check if the table exists already
    cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'block');")

    exists = cur.fetchone()

    log.info("exists: %s" % exists)

    if exists[0] is True:
        return False

    # Open initial.sql
    initial_file = os.path.join(os.path.dirname(__file__), 'sql', 'initial.sql')

    log.info("Creating initial schema with %s" % initial_file)

    try:
        with open(initial_file) as sql_file:
            cur.execute(sql_file.read())

        conn.commit()

    except psycopg2.Error as e:
        if "already exists" not in str(e):
            log.exception("Invalid SQL file for initial data schema")
            conn.rollback()
            cur.close()
            conn.close()
            sys.exit(51)
        else:
            conn.rollback()

    # Cleanup
    cur.close()
    conn.close()

    return True
