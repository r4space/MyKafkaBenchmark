""" Module to handle initial stages of installation """
#External imports
from urllib2 import urlopen, URLError, HTTPError
import platform
import configparser

import fabric.contrib.files
import fabric.api
import fabric.utils
fabric.api.settings.warn_only=True

import logging
logger = logging.getLogger('fabricsoodt.setup')


def readconfig(filename):
	""" Reads the config file and returns a dictionary of sections and their details (also as dictionaries)"""
	configuration = {}

	config = configparser.ConfigParser()
	config.read(filename)

	for section in config:
		if section == "DEFAULT": continue
		parameters = {}
		for key in config[section]:
			try:
				parameters[key]=eval("config[section].getint(key)")

			except ValueError:
				try:
					parameters[key]=eval("config[section].getboolean(key)")
				except ValueError:
					parameters[key]=config[section][key].strip()

		configuration[section]=parameters

	return configuration

def download(url, destination):
	""" Fetches a generic URL to disk, creating the destination directory if necessary, return success or fail. """
	resultName = destination+url.split('/')[-1:][0]

	if not fabric.contrib.files.exists(destination):
		ret=fabric.api.run("mkdir -p " + destination)
		if ret.failed:
			fabric.utils.abort("Failed to make directory {0}, likely due to permissions issue, aborting".format(destination))
	elif fabric.contrib.files.exists(resultName) and not fabric.contrib.console.confirm("File {} already found, re-download?".format(resultName)):
		return 1

	try:
		f = urlopen(url)
		with open(resultName,"wb") as output_file:
			logger.info ("\n> Downloading {0} to {1}".format(url, destination))
			output_file.write(f.read())
		return 1
	except HTTPError, e:
		logger.error("\n> HTTP Error: ", e.code, url)
		return 0
	except URLError, e:
		logger.error("\n> URL Error: ", e.reason, url)
		return 0

def getos():
	""" Return 6 letter lowercase version of current OS """
	dist = platform.linux_distribution()
	return dist[0].lower()[0:6]

def setEnvs(string,user):
	envvars = dict(pair.split(":") for pair in string.split(","))
	for var in envvars:
		ret=fabric.api.run("echo \"export {0}={1}\" >> /home/{2}/.bash_profile".format(var,envvars[var],user))
		logger.info("\n> Set {0} = {1}".format(var,envvars[var]))
		if ret.failed:
			fabric.utils.abort("Failed to set env variables{0}, aborting".format(var))










