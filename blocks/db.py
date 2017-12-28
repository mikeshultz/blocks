""" Database models and utilities """
import os
import sys
import logging
import random
import psycopg2
from datetime import datetime
from eth_utils.address import is_address
from rawl import RawlBase
from .config import LOGGER

log = LOGGER.getChild('db')


class InvalidRange(IndexError): pass
class LockExists(Exception): pass

class BlockModel(RawlBase):
    def __init__(self, dsn: str):
        super(BlockModel, self).__init__(dsn, table_name='block', 
            columns=['block_number', 'block_timestamp', 'difficulty', 'hash',
                     'miner', 'gas_used', 'gas_limit', 'nonce', 'size'], 
            pk_name='block_number')

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

class TransactionModel(RawlBase):
    def __init__(self, dsn: str):
        super(TransactionModel, self).__init__(dsn, table_name='transaction', 
            columns=['hash', 'block_number', 'from_address', 'to_address',
                     'value', 'gas_price', 'gas_limit', 'nonce', 'input'],
            pk_name='hash')

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

class LockModel(RawlBase):
    """ Model representing a lock in the DB """

    def __init__(self, dsn: str):
        super(LockModel, self).__init__(dsn, table_name='lock', 
            columns=['lock_id', 'name', 'updated', 'pid'], pk_name='lock_id')

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

    def update_lock(self, lock_id):
        return self.query(
            "UPDATE lock SET updated = now() WHERE lock_id = {};",
            lock_id, commit=True)

    def add_lock(self, name, pid=random.randint(0,999)):
        return self.insert_dict({
            "name": name,
            "pid": pid,
            }, commit=True)

    def lock(self, name, pid=random.randint(0,999)):
        res = self.check_lock(name)
        if len(res) > 0:
            if res[0].pid != pid:
                raise LockExists("Lock already exists")
            return True
        else:
            self.lock_id = self.add_lock(name, pid)
            if self.lock_id:
                log.debug("Created lock #%s" % self.lock_id)
                self.name = name
                return True
            else:
                log.warn("Failed to create new lock!")
                return False

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
