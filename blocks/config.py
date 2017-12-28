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

"""
# Disable the pylint rule for Invalid Constant because that's really annoying
# pylint: disable=C0103
import os
import sys
import logging
import configparser

# Deal with configuration
CONFIG = configparser.ConfigParser()
found_config = False

INI_NAME = 'blocks.ini'
system_conf = os.path.join('/etc', INI_NAME)
if os.path.isfile(system_conf):
    CONFIG.read(system_conf)
    found_config = True

user_conf = os.path.expanduser(os.path.join('~', '.config', 'blocks.ini'))
if os.path.isfile(user_conf):
    CONFIG.read(user_conf)
    found_config = True

# No config?
if found_config is False:
    print("No configuration found", file=sys.stderr)
    sys.exit(1)

# Log level can be gotten from here: 
LEVEL = {
    'CRITICAL': 50,
    'ERROR':    40,
    'WARNING':  30,
    'INFO':     20,
    'DEBUG':    10
}
logging.basicConfig(stream=sys.stdout, level=LEVEL.get(CONFIG['default'].get('loglevel'), 'WARNING'))
LOGGER = logging.getLogger('blocks')

# Create DSN string for DB
DSN = "postgresql://%s:%s@%s:%s/%s" % (
    CONFIG['postgresql']['user'],
    CONFIG['postgresql']['pass'],
    CONFIG['postgresql'].get('host', "localhost"),
    CONFIG['postgresql'].get('port', 5432),
    CONFIG['postgresql'].get('name', "blocks")
    )

# Ethereum config
ETHEREUM_CONFIG = CONFIG['ethereum']
JSONRPC_NODE = None
if ETHEREUM_CONFIG:
    JSONRPC_NODE = ETHEREUM_CONFIG.get('node', fallback="http://localhost:8545/")

if not JSONRPC_NODE:
    LOGGER.critical("Ethereum node must be provided")
    sys.exit(3)