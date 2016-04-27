""" Module containing functions for the extraction, building, and pushing of applications """
# External imports
import platform
import logging

# Internal imports
import fabricsoodt.setup
import fabric.api

fabric.api.env.warn_only = True

logger = logging.getLogger("fabricsoodt.build")


def extract(fullname, destination):
    """ Extracts an application to a destination unsing the appropriate tar or unzip based on its file extension """
    if fullname.endswith('.tgz'):  # tar gzip
        # os.system('tar -xzvf %s -C %s' % (location+application,location))
        fabric.api.local('tar --skip-old-files -xzf ' + fullname + ' -C ' + destination)
        dirname = fullname.split('/')[-1:][0][:-4]
        logger.info(
            "\n> Untared {0} to {1}, returning resulting directory name: {2}".format(fullname, destination, dirname))
        return dirname

    if fullname.endswith('.tar.gz'):  # tar gzip
        # os.system('tar -xzvf %s -C %s' % (location+application,location))
        fabric.api.local('tar --skip-old-files -xzf ' + fullname + ' -C ' + destination)
        dirname = fullname.split('/')[-1:][0][:-7]
        logger.info(
            "\n> Untared {0} to {1}, returning resulting directory name: {2}".format(fullname, destination, dirname))
        return dirname

    elif fullname.endswith('.zip'):  # zip
        # os.system('unzip -x %s -d %s' % (location+application,location))
        fabric.api.local(' unzip -xn ' + fullname + ' -d ' + destination)
        dirname = fullname.split('/')[-1:][0][:-4]
        logger.info(
            "\n> Unzipped {0} to {1}, returning resulting directory name: {2}".format(fullname, destination, dirname))
        return dirname
