""" Setup relationalblocks """
import os.path
from setuptools import setup, find_packages

__DIR__ = os.path.abspath(os.path.dirname(__file__))

setup(
    name='relationalblocks',
    version='0.0.1b1',
    description='Service that puts the Ethereum blockchain into PostgreSQL',
    url='https://github.com/mikeshultz/relationalblocks',
    author='Mike Shultz',
    author_email='mike@mikeshultz.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='ethereum',
    packages=find_packages(exclude=['build', 'dist']),
    package_data={'': ['README.md', 'relationalblocks/sql/initial.sql']},
    install_requires=[
        'rawl>=0.1.1b2',
        'Flask>=0.12.2',
        'Flask-JSONRPC>=0.3.1',
        'flask-cors>=3.0.3',
        'uwsgi>=2.0.15',
        'web3>=3.16.4',
        'cytoolz>=0.9.0', # Due to a requirement in eth-tester
        'eth_utils==0.7.3',
    ],
    entry_points={
        'console_scripts': [
            'blockconsumer = relationalblocks.consumer:main'
        ]
    },
)
