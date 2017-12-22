""" 
config.py handles all configuration 

User configuration should go in an INI file in either of the following(in 
rising precidence):

 - /etc/relationalblocks.ini
 - ~/.config/relationalblocks.ini

Example: 

[default]
log_level = debug

[postgresql]
host = db.example.com
port = 5432
user = myuser
pass = my$ecretPASS
name = relationblocks

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

INI_NAME = 'blocktime.ini'
system_conf = os.path.join('/etc', INI_NAME)
if os.path.isfile(system_conf):
    CONFIG.read(system_conf)
    found_config = True

user_conf = os.path.expanduser(os.path.join('~', '.config', 'blocktime.ini')
if os.path.isfile(user_conf):
    CONFIG.read(user_conf)
    found_config = True

# No config?
if found_config is False:
    print("No configuration found", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(stream=sys.stdout, level=CONFIG['default'].get('loglevel'))
LOGGER = logging.getLogger('blocktime')

# Create DSN string for DB
DSN = "postgresql://%s:%s@%s:%s/%s" % (
    CONFIG['postgresql']['user'],
    CONFIG['postgresql']['pass'],
    CONFIG['postgresql'].get('host', "localhost"),
    CONFIG['postgresql'].get('port', 5432),
    CONFIG['postgresql'].get('name', "relationalblocks")
    )

# Ethereum config
ETHEREUM_CONFIG = CONFIG.get('ethereum')
JSONRPC_NODE = None
if ETHEREUM_CONFIG:
    JSONRPC_NODE = ETHEREUM_CONFIG.get('node', fallback="http://localhost:8545/")

if not JSONRPC_NODE:
    LOGGER.critical("Ethereum node must be provided")
    sys.exit(3)