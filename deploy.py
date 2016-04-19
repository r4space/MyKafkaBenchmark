"""
Used to deploy Kafka benchmarking tests across a homogenous cluster
Usage: python deploy.py [ install | clean | startBrokers | stopBrokers | startProducers | startConsumers | stopProducers | stopConsumers | runTest X | regenerateData] [ config_file ]
"""
import sys
import logging.config
import json
import os
import argparse

import clusterkafka

parser = argparse.ArgumentParser()
parser.add_argument("-cl", "--colourLog", help="Activate colour logging", action="store_true")
parser.add_argument("command", help="Enter a command: install | clean | startBrokers | stopBrokers | startProducers | startConsumers | stopProducers | stopConsumers | runTest X | regenerateData")
parser.add_argument("config", help="Supply a path to a configuration file")
args = parser.parse_args()

# Setup Logging
if args.colourLog:
    try:
        import coloredlogs
    except ImportError as e:
        logging.error("Error: {0}".format(e))
        print(
            "python module coloredlogs is not installed on this system, please install $ pip install coloredLogs or remove the --colouredLogs flag and run again.")
        sys.exit(1)

    print('Color logging enabled')
    clusterkafka.setup.setup_logging()
    coloredlogs.install(level='DEBUG')
else:
   clusterkafka.etup.setup_logging()

#  External imports
try:
    import yarn
    import pystache
    import configparser
    logging.info("Successfully imported external modules")

except ImportError as e:
    logging.error("Error: {0}".format(e))
    print("Please install the necessary python modules: 'pip install --user -r requirements.txt'")
    sys.exit(1)

# Acquire Configuration Information
configuration = clusterkafka.setup.getconfig(args.config)
operator = clusterkafka.operate.operate(configuration)

if args.command() == "install":
    logging.info("> Beginning install of SOODT")
    operator.install()

elif args.command() == "clean":
    logging.info("Cleaning SOODT install directories")
    operator.clean()

elif args.command() == 'startBrokers':
    logging.info("Starting components")
    operator.startBrokers()

elif args.command() == 'startProducers':
    logging.info("Starting components")
    operator.startProducers()

elif args.command() == 'startConsumers':
    logging.info("Starting components")
    operator.startConsumers()

elif args.command() == 'stopBrokers':
    logging.info("Stopping components")
    operator.stopBrokers()

elif args.command() == 'stopProducers':
    logging.info("stoping components")
    operator.stopProducers()

elif args.command() == 'stopConsumers':
    logging.info("stopping components")
    operator.stopConsumers()

elif args.command() == 'runTest':
    logging.info("stopping test")
    operator.runTest()

elif args.command() == 'regenerateData':
    logging.info("CreatingData")
    operator.createData()
else:
    logging.error("Error: Unrecognised command")
    sys.exit(1)
