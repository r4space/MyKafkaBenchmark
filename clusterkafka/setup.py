""" Module to handle initial stages of installation """
# External imports
import urllib.request
import platform
import configparser
import logging
from yarn import api
import os
import json

log = logging.getLogger(__name__)

def setup_logging(
        default_path='./configs/LoggerConfig.json',
        default_level=logging.INFO):
    """ Setup Logging Configuration"""

    path = default_path

    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
            logging.config.dictConfig(config)

    else:
        print("No logger config file found at {0}, using default level of INFO".format(path))
        log.basicConfig(level=default_level)


def getconfig(filename):
    """ Reads the config file and returns a dictionary of sections and their details (also as dictionaries) """
    configuration = {}

    config = configparser.ConfigParser()
    config.read(filename)
    config.sections()
    log.info("Reading in configuration")
    for section in config:
        if section == "DEFAULT": continue
        parameters = {}
        for key in config[section]:
            try:
                parameters[key] = eval("config[section].getint(key)")

            except ValueError:
                try:
                    parameters[key] = eval("config[section].getboolean(key)")
                except ValueError:
                    parameters[key] = config[section][key].strip()

        configuration[section] = parameters

    return configuration

@api.parallel
def setEnvs(string, user):
    envvars = dict(pair.split(":") for pair in string.split(","))
    for var in envvars:
        api.run("echo \"export {0}={1}\" >> /home/{2}/.bash_profile".format(var, envvars[var], user))

    log.info("\n Set env variable: {0} = {1}".format(var, envvars[var]))

@api.parallel
def push_extract(source, destination):
    """Puts and extracts a *.tgz file in a remote directory after checking it exists or creating it """
    taronly=source.split("/")[-1]
    log.info("\nPushing kafka tgz to nodes and extracting")
    #Check for destination folder
    if api.run("[ -d \"{}\" ] && echo 1".format(destination)) != "1":
        api.run("mkdir {}".format(destination))

    #Check for tgz already existing there and then extract
    if api.run("[ -f \"{}\" ] && echo 1".format(destination+taronly)) == "1":
        api.run('tar --skip-old-files -xzf {} -C {} '.format(destination + taronly, destination))
        return
    else:
        api.put(source, destination+taronly)
        api.run('tar --skip-old-files -xzf {} -C {} '.format(destination + taronly, destination))
        return

def check_nodes(ip):
    """ Checks if the local machine can reach a given IP returns
    :param ip:
    :return True/False:
    """
    log.info("\nChecking {} is reachable from host node. - ".format(ip))
    str=api.local("nc -z -w1 {} 22 && echo $?".format(ip))
    if str == "0":
        log.info("Success")
        return True
    else:
        log.info("\nFailed to reach {}".format(ip))
        return False




