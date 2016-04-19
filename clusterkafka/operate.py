""" Module containing:
clean(),
install(),
cleanBroker(directory)
startBrokers(),
stopBrokers(),
startProducers(),
startConsumers(),
stopProducers(),
stopConsumers(),
runTest(),
createData()
"""
import sys
# from yarn.api import env, cd, run, local
from yarn import api
import logging


class operate:
    def __init__(self, configuration):
        self.configuration = configuration

    def cleanBroker(directory):
        """ Removes contents of a directory """
        ret = run("rm -r {0}".format(directory))
        if ret.failed:
            logging.error("Failed to clean broker node directory: {0}".format(directory))

    def clean(self):
        """ Cleans the logs, templates and downloads folders """
        logging.info("Cleaning clusterkafka/templates")
        api.local("rm -r ./templates/myid")
        api.local("rm -r ./templates/*.properties")

        logging.info("Cleaning ./logs/CK_logs/")
        api.local("rm -r ../logs/CK_logs/*")

        api.local("rm -r ./*.pyc")  # TODO remove this line - this is just for during dev

    def install(self):
        """Download, configure, and distribute a kafka configuration"""

        api.env.user = self.configuration['CONFIGS']['user']
        api.env.host_string = 'localhost'

        allIPs = set()
        print("nodeIPs: {}".format(self.configuration['KAFKA']['nodes']))
        nodeIPs = map(lambda z: z.strip(" "), self.configuration['KAFKA']['nodes'].split(","))
        print("nodeIPs: {}".format(nodeIPs))
        # self.configuration['KAFKA']['NODES'] = nodeIPs  # Create an entry containing the IPs in a python list
        # [allIPs.add(x) for x in nodeIPs]  # Create an ALLnodesIPs list
        # fabric.api.env.roledefs['ALLnodesIPs'] = allIPs


        #        # Set local env variables:
        #        fabric.api.execute(fabricsoodt.setup.setEnvs, self.configuration['CONFIGS']['envvars'], configuration['CONFIGS']['user'],roles=['login'])
        #        logging.info("Set envars on broker machines")
        #
        #        # DOWNLOADS
        #        pwd = fabric.api.local("pwd", capture=True)
        #        downloads = pwd + "/downloads/"
        #
        #        if fabric.api.execute(fabricsoodt.setup.download, self.configuration['KAFKA']['url'], downloads, roles=['login']):
        #            logging.info("\n> Downloaded: {0} to {1}".format('KAFKA', downloads))
        #
        #        else:
        #            logging.error("\n> Failed to download{0}".format('KAFKA'))
        #            if not fabric.contrib.console.confirm("Do you wish to continue with out installing {0}".format('KAFKA')):
        #                fabric.api.abort("Aborting at users request")
        #            else:
        #                logging.info("\n> Continuing without {0}".format('KAFKA'))
        #
        #        # EXTRACT
        #        tarPath = downloads + self.configuration['KAFKA']['url'].split('/')[-1:][0]
        #        # Extract returns the resulting folder name, capture to self.configuration
        #        self.configuration['KAFKA']['folderName'] = fabricsoodt.build.extract(tarPath, downloads)
        #
        #        # Set Application HOME path
        #        self.configuration['KAFKA']['HOME'] = configuration['CONFIGS']['destination'] + configuration['KAFKA']['folderName']
        #
        #        destination = self.configuration['CONFIGS']['destination']
        #        fabric.api.execute(fabricsoodt.distribute.existsCreate, destination, roles=['ALLnodesIPs'])
        #
        #        # Distribute
        #        source = downloads + self.configuration['KAFKA']['folderName']
        #        logging.info("\n> Transfering {0} to nodes: {1}".format(source, destination))
        #        fabric.api.execute(fabricsoodt.distribute.transfer, source, destination, roles=["KAFKAnodeIPs"])
        #
        #        # Distribute self.configuration files
        #        for server in range(1, len(self.configuration['KAFKA']['NODES']) + 1):
        #            fabric.api.execute(fabricsoodt.distribute.configKafka, self.configuration['KAFKA'],server, hosts=nodeIPs[server-1])


        logging.info("\n> Kafka deployment sucessfully completed")

    def startBrokers():
        ''' Start components based on config file requests and logical order '''
        # Returns the contents of provided ini file as a dictionary of dictionaries
        self.configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
        logging.info("\n> StartBrokers() Reading self.configuration file: {0}".format(sys.argv[2]))

        fabric.api.env.colorize_error = True
        fabric.api.env.user = self.configuration['CONFIGS']['user']

        # Create an entry in self.configuration libraries containing app specific IPs in a python list
        nodeIPs = map(lambda z: z.strip(" "), self.configuration['KAFKA']['nodes'].split(","))
        self.configuration['KAFKA']['NODES'] = nodeIPs
        logging.info(">Captured node IPs in config: " + str(nodeIPs))

        logging.info("Cleaning broker cluster")
        fabric.api.execute(cleanBroker, self.configuration['KAFKA']['logsdir'], hosts=nodeIPs)
        fabric.api.execute(cleanBroker, self.configuration['KAFKA']['zkdatadir'], hosts=nodeIPs)

        # Cannot implement below without restructuring as configKafka relies on config['KAFKA']['HOME'] being set which is only done in operate.install()....
        logging.info("\n>Refresh kafka self.configuration")
        fabric.api.execute(fabricsoodt.distribute.configKafka, self.configuration['KAFKA'], hosts=nodeIPs)

        logging.info("\n> Starting Kafka")
        fabricsoodt.start.startupKafka(self.configuration['KAFKA'])

    def stopBrokers():
        """ Stop components based on config file requests and logical order """
        fabric.api.env.colorize_error = True
        # Returns the contents of provided ini file as a dictionary of dictionaries
        self.configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
        fabric.api.env.user = self.configuration['CONFIGS']['user']
        logging.info("\n> stopBrokers() Reading self.configuration file: {0}".format(sys.argv[2]))
        # Create an entry in self.configuration libraries contaning app specific IPs in a python list
        nodeIPs = map(lambda z: z.strip(" "), self.configuration['KAFKA']['nodes'].split(","))
        self.configuration['KAFKA']['NODES'] = nodeIPs

        logging.info("\n> Stopping Kafka")
        fabricsoodt.stop.stopKafka(self.configuration['KAFKA'])

    def runTest():
        """ Initialises producer and consumer nodes and starts up first the producers and then the conumers """
        # TODO fix runTest
        # Get config
        # Returns the contents of provided ini file as a dictionary of dictionaries
        self.configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
        logging.info("\n> StartProducers() Reading self.configuration file: {0}".format(sys.argv[2]))
        pwd = fabric.api.local("pwd", capture=True)
        downloads = pwd + "/downloads/"
        logging.info(">>Set downloads to{}".format(downloads))

        # Setup Fabric fabric.api.envs
        fabric.api.env.colorize_error = True
        fabric.api.env.user = self.configuration['CONFIGS']['user']

        # Extract list of consumer node ips into python list
        BrokerIPs = map(lambda x: x.strip(" "), self.configuration['KAFKA']['nodes'].split(","))
        ConsumerIPs = map(lambda x: x.strip(" "), self.configuration['CONSUMERS']['nodes'].split(","))
        ProducerIPs = map(lambda x: x.strip(" "), self.configuration['PRODUCERS']['nodes'].split(","))
        fabric.api.env.roledefs['brokers'] = BrokerIPs
        fabric.api.env.roledefs['consumers'] = ConsumerIPs
        fabric.api.env.roledefs['producers'] = ProducerIPs

        logging.info("This is roledefs " + str(fabric.api.env.roledefs))

        # Create zookeeper connect ip:port list
        zocC = ""
        zocP = ""
        for ip in BrokerIPs:
            zocC = zocC + str(ip) + ":" + str(self.configuration['KAFKA']['zkport']) + ","
            zocP = zocP + str(ip) + ":" + str(self.configuration['KAFKA']['sport']) + ","
        zocC = zocC[:-1]
        zocP = zocP[:-1]
        self.configuration['TEST']['zocC'] = zocC
        self.configuration['TEST']['zocP'] = zocP

        logging.info("Constructed zookeeper is: {0}".format(zocC))

        # Set $HTK_HOME remotely
        logging.info("\n----------------Setting HTK_HOME----------------")
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar,
                           "HTK_HOME=" + self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/",
                           roles=['consumers'])
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar,
                           "HTK_HOME=" + self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/",
                           roles=['producers'])
        logging.info("\n> Set HTK_HOME ={0}".format(self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/"))

        logging.info("\n----------------Setting K_HOME----------------")
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar, "K_HOME=/home/jwyngaard/.local/kafka_2.10-0.8.2.1",
                           roles=['consumers'])
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar, "K_HOME=/home/jwyngaard/.local/kafka_2.10-0.8.2.1",
                           roles=['producers'])
        logging.info("\n> Set K_HOME ={0}".format(self.configuration['CONFIGS']['destination'] + "Kafka-benchmark/"))

        # Distribute HT-kafka-benchmark to Consumer and producer nodes
        logging.info("\n----------------Distributing HTKafka-benchmark----------------")
        HTKsrc = downloads + "build"
        HTKdest = self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/"
        logging.info("Transfering {0} to {1}".format(HTKsrc, HTKdest))
        fabric.api.execute(fabricsoodt.distribute.copy, HTKsrc, HTKdest, roles=['consumers'])
        fabric.api.execute(fabricsoodt.distribute.copy, HTKsrc, HTKdest, roles=['producers'])

        # Run a test
        fabricsoodt.test.GoTest1(self.configuration)
        print("FINISHED TEST")

    # ---------------------- ---------------------
    #    # Start topic(s)
    #    logging.info("\n----------------Starting topics----------------")
    #    fabricsoodt.test.startupTopics(self.configuration['TEST'])
    #
    #    #	#Start producers=(s)
    #    logging.info("\n----------------Starting producers----------------")
    #    fabricsoodt.test.startupProducers(self.configuration)
    #
    #    time.sleep(10)
    #    # Start consumer(s)
    #    logging.info("\n---------------- Starting consumers----------------")
    #    fabricsoodt.test.startupConsumers(self.configuration)

    #
    #	#Collate results()
    #	#Produce report()
    #	#Delete topics()
    #   #Shutdown Consumers()

    def createData():
        self.configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
        logging.info("\n---------------- Creating data on producer nodes ----------------")
        logging.info("\n> createData() reading self.configuration file: {0}".format(sys.argv[2]))
        of = self.configuration['TEST']['of']
        bs = self.configuration['TEST']['bs']
        count = self.self.configuration['TEST']['count']
        NoFiles = self.configuration['TEST']['NoFiles']
        PNODES = map(lambda z: z.strip(" "), self.configuration['PRODUCERS']['nodes'].split(","))
        fabricsoodt.test.createData(of, bs, count, NoFiles, PNODES)

    def startProducers():
        pass

    # TODO define startProducers
    def startConsumers():
        pass

    # TODO define startConsumers

    def stopProducers():
        pass

    # TODO define stopProducers

    def stopConsumers():
        pass
        # TODO defin stopConsumers
