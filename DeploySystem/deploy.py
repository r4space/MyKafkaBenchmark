from __future__ import print_function

'''
Used to deploy Kafka benchmarking tests across a homogenous cluster
Usage: python deploy.py [ install | clean | startBrokers | stopBrokers | startProducers | startConsumers | stopProducers | stopConsumers | runTest X | regenerateData] [ config_file ]
'''

import sys
import os
import logging

# Setup logging
#logger = logging.getLogger("fabricsoodt")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
FH = logging.FileHandler('./logs/SOODTInstall.log')
FH.setLevel(logging.DEBUG)
FH.setFormatter(formatter)
CH = logging.StreamHandler()
CH.setLevel(logging.DEBUG)
CH.setFormatter(formatter)
logger.addHandler(FH)
logger.addHandler(CH)

try:
    import fabric
    import configparser
    import pystache
except ImportError, e:
    logger.error("Error: {0}".format(e))
    print("Please install the necessary python modules: 'pip install --user -r requirements.txt'")
    sys.exit(1)

import fabricsoodt.operate

# Check correct calling
Usage_string = "Usage: python deploy.py [ install | clean | startBrokers | stopBrokers | startProducers | startConsumers | stopProducers | stopConsumers | runTest X | regenerateData] [ config_file ]"

if len(sys.argv) < 3:
    logger.error("Error: Incorrect call syntax")
    print(usage_string)
    sys.exit(1)

elif not os.path.exists(sys.argv[2]):
    pwd = os.getcwd()
    path_to_file = pwd + '/' + sys.argv[2]
    logger.error("Error: Configuration file: {}, does not exist".format(path_to_file))
    sys.exit(1)

else:

    if sys.argv[1].lower() == "install":
        logger.info("> Begining install of SOODT")
        fabricsoodt.operate.install()

    elif sys.argv[1].lower() == "clean":
        logger.info("Cleaning SOODT install directories")
        fabricsoodt.operate.clean()

    elif sys.argv[1].lower() == 'startBrokers':
        logger.info("Starting components")
        fabricsoodt.operate.startBrokers()

    elif sys.argv[1].lower() == 'startProducers':
        logger.info("Starting components")
        fabricsoodt.operate.startProducers()

    elif sys.argv[1].lower() == 'startConsumers':
        logger.info("Starting components")
        fabricsoodt.operate.startConsumers()

    elif sys.argv[1].lower() == 'stopBrokers':
        logger.info("Stopping components")
        fabricsoodt.operate.stopBrokers()

    elif sys.argv[1].lower() == 'stopProducers':
        logger.info("stoping components")
        fabricsoodt.operate.stopProducers()

    elif sys.argv[1].lower() == 'stopConsumers':
        logger.info("stopping components")
        fabricsoodt.operate.stopConsumers()

    elif sys.argv[1].lower() == 'runTest':
        logger.info("stopping test")
        fabricsoodt.operate.runTest()

    elif sys.argv[1].lower() == 'regenerateData':
        logger.info("CreatingData")
        fabricsoodt.operate.createData()
    else:
        print("Error: Unrecognised command")
        print(usage_string)
        sys.exit(1)
