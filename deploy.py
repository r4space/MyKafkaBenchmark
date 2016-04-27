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
parser.add_argument("command", help="Enter a command: install | clean | cleanBrokers | startBrokers | stopBrokers | startProducers | startConsumers | stopProducers | stopConsumers | runTest X | regenerateData")
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
   clusterkafka.setup.setup_logging()

log = logging.getLogger(__name__)

#  External imports
try:
    import yarn
    import pystache
    import configparser
    log.info("Successfully imported external modules")

except ImportError as e:
    log.error("Error: {0}.  \nPlease install the necessary python modules: 'pip install --user -r requirements.txt'".format(e))
    sys.exit(1)

# Acquire Configuration Information
configuration = clusterkafka.setup.getconfig(args.config)
operator = clusterkafka.operate.operate(configuration)

if args.command == "install":
    log.info("Beginning install of distributed Kafka")
    operator.install()

elif args.command == "clean":
    log.info("Cleaning local install directories")
    operator.clean()

elif args.command == "cleanBrokers":
    log.info("Cleaning remote kafka broker directories")
    operator.clean_brokers()

elif args.command == 'startBrokers':
    log.info("Starting components")
    operator.start_kbrokers()

elif args.command == 'stopBrokers':
    log.info("Stopping Kafka Broker nodes")
    operator.stop_kbrokers()

elif args.command == 'startProducers':
    log.info("Starting components")
    operator.startProducers()

elif args.command == 'startConsumers':
    log.info("Starting components")
    operator.startConsumers()


elif args.command == 'stopProducers':
    log.info("stoping components")
    operator.stopProducers()

elif args.command == 'stopConsumers':
    log.info("stopping components")
    operator.stopConsumers()

elif args.command == 'runTest':
    log.info("stopping test")
    operator.runTest()

elif args.command == 'regenerateData':
    log.info("CreatingData")
    operator.createData()
else:
    log.error("Error: Unrecognised command")
    sys.exit(1)
