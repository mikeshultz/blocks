""" Setup relationalblocks """
import os.path
from setuptools import setup, find_packages

__DIR__ = os.path.abspath(os.path.dirname(__file__))


def find_version(*file_paths):
    version = None
    with open('VERSION') as _version_file:
        version = _version_file.read()
    return version


setup(
    name='blocks',
    version=find_version(),
    description='Service that puts the Ethereum blockchain into PostgreSQL',
    url='https://github.com/mikeshultz/blocks',
    author='Mike Shultz',
    author_email='mike@mikeshultz.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='ethereum',
    packages=find_packages(exclude=['build', 'dist']),
    package_data={'': ['README.md', 'sql/initial.sql']},
    install_requires=[
        'rawl>=0.3.4',
        'Flask>=0.12.2',
        'Flask-JSONRPC>=0.3.1',
        'flask-cors>=3.0.3',
        'uwsgi>=2.0.15',
        'hexbytes',
        'eth-utils<2.0.0,>=1.2.0',
        'web3>=4.2.1,<5',
    ],
    # pip install -e .[dev]
    extras_require={
        'dev': [
            'flake8>=3.8.4',
            'mypy>=0.790'
        ]
    },
    # Every damned Ethereum python package in PyPi seems afflicted with a pypandoc
    # related issue.  For some reason, their releases on github work just fine, so
    # for now, we use these:
    dependency_links=[
        'https://github.com/carver/hexbytes/archive/v0.1.0.tar.gz#egg=hexbytes',
        'https://github.com/ethereum/eth-hash/archive/v0.1.3.tar.gz#egg=eth-hash-0.1.3',
        'https://github.com/ethereum/eth-account/archive/v0.2.2.tar.gz#egg=eth-account-0.2.2',
        'https://github.com/ethereum/eth-rlp/archive/v0.1.2.tar.gz#egg=eth-rlp-0.1.2',
    ],
    entry_points={
        'console_scripts': [
            'blocksapi = blocks:api',
            'conductor = blocks.cli:start_conductor',
            'blockconsumer = blocks.cli:start_block_consumer',
            'txconsumer = blocks.cli:start_transaction_consumer',
        ]
    },
)
