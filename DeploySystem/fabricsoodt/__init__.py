from __future__ import print_function
import logging
""" This package enables easy installation of Streeming OODT accross a homogenous cluster 

Package depends on:
	fabric 
	configparser
	pystache

packge includes modules:
	setup
	building
	distributing

"""
__all__ = ['setup','build','distribute','operate','test']
#Test availability of required nonStd external imports
try:
	import pystache
	import configparser
	import fabric
#	from fabric.api import env, local, lcd 
#	from fabric.contrib.files import exists
#	from fabric.contrib.console import confirm
#	from fabric.utils import abort
except ImportError, e:
	print("Error: {0}".format(e)) 

logger = logging.getLogger('foo').addHandler(logging.NullHandler())
