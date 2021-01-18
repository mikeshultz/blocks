"""
config.py handles all configuration

User configuration should go in an INI file in either of the following(in
rising precidence):

 - /etc/blocks.ini
 - ~/.config/blocks.ini

Example:

[default]
loglevel = DEBUG

[postgresql]
host = db.example.com
port = 5432
user = myuser
pass = my$ecretPASS
name = blocks

[ethereum]
node = http://localhost:8545/

Or env vars:

LOG_LEVEL
JSONRPC_NODE
PGUSER
PGPASSWORD
PGHOST
PGPORT
PGDATABASE

"""
# Disable the pylint rule for Invalid Constant because that's really annoying
# pylint: disable=C0103
import os
import sys
import logging
import configparser

from typing import Any


def env_or_ini(env_name: str, confparser: configparser.ConfigParser, ini_section: str,
               ini_key: str, fallback: Any = None) -> Any:
    """ Coalesce configuration from an env var or ini config setting

    :param env_name: :code:('str') string name of the env var
    :param ini_section: :code:('str') string INI section name to look in
    :param ini_key: :code:('str') string setting key to look for within the section
    """
    if os.environ.get(env_name):
        return os.environ[env_name]
    if ini_section in confparser and ini_key in confparser[ini_section]:
        return confparser[ini_section].get(ini_key)
    return fallback


"""

Attempt to load the configuration files

"""
CONFIG = configparser.ConfigParser()

INI_NAME = 'blocks.ini'
system_conf = os.path.join('/etc', INI_NAME)
if os.path.isfile(system_conf):
    print("Loading configuration from %s" % system_conf)
    CONFIG.read(system_conf)

user_conf = os.path.expanduser(os.path.join('~', '.config', INI_NAME))
if os.path.isfile(user_conf):
    print("Loading configuration from %s" % user_conf)
    CONFIG.read(user_conf)

"""

Configure logging and set the log level

"""
LEVEL = {
    'CRITICAL': 50,
    'ERROR':    40,
    'WARNING':  30,
    'INFO':     20,
    'DEBUG':    10
}
conf_loglevel = env_or_ini('LOG_LEVEL', CONFIG, 'default', 'loglevel', 'WARNING')
logging.basicConfig(stream=sys.stdout, level=LEVEL.get(conf_loglevel))
LOGGER = logging.getLogger()
LOGGER.setLevel(LEVEL.get(conf_loglevel, 20))
logging.getLogger('rawl').setLevel(logging.INFO)

"""

Assemble the PostgreSQL connection DSN

"""
pg_vars = {
    'user': env_or_ini('PGUSER', CONFIG, 'postgresql', 'user'),
    'pass': env_or_ini('PGPASSWORD', CONFIG, 'postgresql', 'pass'),
    'host': env_or_ini('PGHOST', CONFIG, 'postgresql', 'host', 'localhost'),
    'port': env_or_ini('PGPORT', CONFIG, 'postgresql', 'port', '5432'),
    'name': env_or_ini('PGDATABASE', CONFIG, 'postgresql', 'name', 'blocks'),
}

assert pg_vars.get('user') is not None, (
    'PGUSER, must be defined as an env var or in INI config and can not be inferred'
)

# Create DSN string for DB
DSN = 'postgresql://{user}:{pass}@{host}:{port}/{name}'.format(**pg_vars)

"""

Set the Ethereum JSON-RPC node endpoint

"""
JSONRPC_NODE = env_or_ini('JSONRPC_NODE', CONFIG, 'ethereum', 'node', 'http://localhost:8545/')
