from argparse import ArgumentParser
from importlib import import_module

from blocks.conductor.api import api, init_flask
from blocks.threads import start_thread
from blocks.enums import WorkerType

ANALYSIS_UTILITIES = ['blocktime']
analysis_modules = {}


def start_conductor():
    """ Startup the conductor """
    init_flask()
    api()


def start_block_consumer():
    """ Startup the block consumer """
    start_thread(WorkerType.BLOCK)


def start_transaction_primer():
    """ Startup the transaction primer """
    start_thread(WorkerType.TX_PRIME)


def start_transaction_consumer():
    """ Startup the transaction consumer """
    start_thread(WorkerType.TX_DETAIL)


def analysis():
    global analysis_modules

    parser = ArgumentParser(description='Run a data analysis utility')
    subparsers = parser.add_subparsers(dest='utility', help='Utility to run')

    for util in ANALYSIS_UTILITIES:
        util_mod = import_module('blocks.analysis.{}'.format(util))
        analysis_modules[util] = util_mod
        analysis_modules[util].add_subparser(subparsers)

    args = parser.parse_args()

    analysis_modules[args.utility].run_cli(args)
