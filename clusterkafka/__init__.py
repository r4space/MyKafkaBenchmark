""" Kafka distribution and testing tool for homogeneous clusters

Package depends on:
	configparser
	pystache
    yarn
    python3

packge includes modules:
	setup
	building
	distributing

"""
__version__ = "0.1.0"  # MAJOR.MINOR.PATCH

from clusterkafka import setup
from clusterkafka import operate
#from clusterkafka import build
#from clusterkafka import distribute


