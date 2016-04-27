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
from yarn import api
from clusterkafka import setup, start, stop, ZKConfigClass, KSConfigClass, ConsumerConfigClass, ProducerConfigClass
import logging
import pystache

log = logging.getLogger(__name__)

class operate:

    def __init__(self, configuration):
        self.configuration = configuration
        # Initialisation
        api.env.user = self.configuration['CONFIGS']['user']
        self.shared = self.configuration['CONFIGS']['shared']
        self.bnodes = self.configuration['KAFKA']['nodes'].split(",")
        self.pnodes = self.configuration['PRODUCERS']['nodes'].split(",")
        self.cnodes = self.configuration['CONSUMERS']['nodes'].split(",")
        self.allnodes=self.bnodes + self.pnodes + self.cnodes
        self.khome = self.configuration['CONFIGS']['installdir']+self.configuration['KAFKA']['url'].split('/')[-1][:-4]+"/"
        if self.shared:
            self.effective_nodes = self.bnodes[0]
        else:
            self.effective_nodes = self.bnodes

        self.render = pystache.Renderer()

    def clean_brokers(self):
        """ Clean kafka broker nodes of logs and configurations """

        logging.info("Cleaning {} ".format(self.configuration['KAFKA']['zkdatadir']))
        for n in self.bnodes:
            api.env.host_string=n
            api.run("rm -r {} 2>/dev/null".format(self.configuration['KAFKA']['zkdatadir']))

        logging.info("Cleaning {} ".format(self.configuration['KAFKA']['logsdir']))
        for n in self.bnodes:
            api.env.host_string=n
            api.run("rm -r {} 2>/dev/null".format(self.configuration['KAFKA']['logsdir']))

    def clean(self):
        """ Cleans the logs, templates and downloads folders """
#            logging.info("Cleaning clusterkafka/temp")
#            api.local("rm -r ./temp/* 2>/dev/null")
# TODO remove comments on above two lines, commented out to stop cleaning removing downloaded kafka
        log.info("Cleaning clusterkafka/templates 2>/dev/null")
        api.local("rm -r ./templates/*.properties 2>/dev/null")

        log.info("Cleaning ./logs/CK_logs/")
        api.local("rm -r ./logs/CK_logs/* 2>/dev/null")

    def install(self):
        """Download, distribute, and configure a distributed kafka configuration"""
        #Check nodes are accessible:
        for n in (self.allnodes):
           if not setup.check_nodes(n):
               sys.exit("Can't reach node:{}, exiting install, check that the IPs in your config file are correct.")
        log.info("All nodes are accessible")
        # Download Kafka
        log.info("Downloading Kafka, please be patient, this shouldn't take more than 1-2 minutes")
        temp = api.local("pwd") + "/temp/"
        api.local("wget -P {} {} 2>/dev/null".format(temp, self.configuration['KAFKA']['url']))
# TODO remove comment on above line so download happens again

        # Push out and extract kafka on nodes
        for n in self.effective_nodes:
            api.env.host_string = n
            setup.push_extract(temp + self.configuration['KAFKA']['url'].split('/')[-1],
                               self.configuration['CONFIGS']['installdir'])

        # Set remote environment variables:
#TODO check if env variable is already set and correct before writing another line to bash_profile
        # K_HOME = "K_HOME:{}/".format(self.configuration['CONFIGS']['installdir']+self.configuration['KAFKA']['url'].split('/')[-1][:-4])
        for n in self.effective_nodes:
            api.env.host_string = n
            setup.setEnvs(self.configuration['CONFIGS']['envs'], self.configuration['CONFIGS']['user'])
            # setup.setEnvs(K_HOME, self.configuration['CONFIGS']['user'])

        # Distribute configuration files to brokers
        self.__config_kafka_brokers()


        log.info("Kafka deployment successfully completed")

    def start_kbrokers(self):
        ''' Starts Kafka Brokers using built in Kafka scripts '''
        self.clean_brokers()

        for n in self.bnodes:
            api.env.host_string=n
            start.start_zserver(self.khome)

        for n in self.bnodes:
            api.env.host_string=n
            start.start_kserver(self.khome)

    def stop_kbrokers(self):
        """ Stop Kafka Broker server scripts """

        for n in self.bnodes:
            api.env.host_string=n
            stop.stop_kserver(self.khome)

        for n in self.bnodes:
            api.env.host_string=n
            stop.stop_zserver(self.khome)


    def runTest():
        """ Initialises producer and consumer nodes and starts up first the producers and then the consumers """
        # TODO fix runTest
        # Get config
        # Returns the contents of provided ini file as a dictionary of dictionaries
        self.configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
        log.info("\n> StartProducers() Reading self.configuration file: {0}".format(sys.argv[2]))
        pwd = fabric.api.local("pwd", capture=True)
        downloads = pwd + "/downloads/"
        log.info(">>Set downloads to{}".format(downloads))

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

        log.info("This is roledefs " + str(fabric.api.env.roledefs))

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

        log.info("Constructed zookeeper is: {0}".format(zocC))

        # Set $HTK_HOME remotely
        log.info("\n----------------Setting HTK_HOME----------------")
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar,
                           "HTK_HOME=" + self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/",
                           roles=['consumers'])
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar,
                           "HTK_HOME=" + self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/",
                           roles=['producers'])
        log.info(
            "\n> Set HTK_HOME ={0}".format(self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/"))

        log.info("\n----------------Setting K_HOME----------------")
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar, "K_HOME=/home/jwyngaard/.local/kafka_2.10-0.8.2.1",
                           roles=['consumers'])
        fabric.api.execute(fabricsoodt.distribute.remoteSetVar, "K_HOME=/home/jwyngaard/.local/kafka_2.10-0.8.2.1",
                           roles=['producers'])
        log.info("\n> Set K_HOME ={0}".format(self.configuration['CONFIGS']['destination'] + "Kafka-benchmark/"))

        # Distribute HT-kafka-benchmark to Consumer and producer nodes
        log.info("\n----------------Distributing HTKafka-benchmark----------------")
        HTKsrc = downloads + "build"
        HTKdest = self.configuration['CONFIGS']['destination'] + "HTKafka-benchmark/"
        log.info("Transfering {0} to {1}".format(HTKsrc, HTKdest))
        fabric.api.execute(fabricsoodt.distribute.copy, HTKsrc, HTKdest, roles=['consumers'])
        fabric.api.execute(fabricsoodt.distribute.copy, HTKsrc, HTKdest, roles=['producers'])

        # Run a test
        fabricsoodt.test.GoTest1(self.configuration)
        print("FINISHED TEST")

    # ---------------------- ---------------------
    #    # Start topic(s)
    #    log.info("\n----------------Starting topics----------------")
    #    fabricsoodt.test.startupTopics(self.configuration['TEST'])
    #
    #    #	#Start producers=(s)
    #    log.info("\n----------------Starting producers----------------")
    #    fabricsoodt.test.startupProducers(self.configuration)
    #
    #    time.sleep(10)
    #    # Start consumer(s)
    #    log.info("\n---------------- Starting consumers----------------")
    #    fabricsoodt.test.startupConsumers(self.configuration)

    #
    #	#Collate results()
    #	#Produce report()
    #	#Delete topics()
    #   #Shutdown Consumers()

    def create_data():
        self.configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
        log.info("\n---------------- Creating data on producer nodes ----------------")
        log.info("\n> createData() reading self.configuration file: {0}".format(sys.argv[2]))
        of = self.configuration['TEST']['of']
        bs = self.configuration['TEST']['bs']
        count = self.self.configuration['TEST']['count']
        NoFiles = self.configuration['TEST']['NoFiles']
        PNODES = map(lambda z: z.strip(" "), self.configuration['PRODUCERS']['nodes'].split(","))
        fabricsoodt.test.createData(of, bs, count, NoFiles, PNODES)

    def start_producers():
        pass

    # TODO define startProducers
    def start_consumers():
        pass

    # TODO define startConsumers

    def stop_producers():
        pass

    # TODO define stopProducers

    def stop_consumers():
        pass
        # TODO defin stopConsumers

    def __config_kafka_brokers(self):
        ''' Configure a Kafka cluster broker machines '''

    # Check required destination folders exists and if not create them on remote nodes
        log.info("Checking necessary Kafka directories {0}, {1} exist".format(self.configuration['KAFKA']['zkdatadir'], self.configuration['KAFKA']['logsdir']))
        for n in self.bnodes:
            api.env.host_string = n
            if api.run("[ -d \"{}\" ] && echo 1".format(self.configuration['KAFKA']['logsdir'])) != "1":
                api.run("mkdir {}".format(self.configuration['KAFKA']['logsdir']))
            if api.run("[ -d \"{}\" ] && echo 1".format(self.configuration['KAFKA']['zkdatadir'])) != "1":
                api.run("mkdir -p {}".format(self.configuration['KAFKA']['zkdatadir']))


    # Add execution permissions to Kafka binaries
        log.info("Make Kafka binaries executable")
        for n in self.effective_nodes:
            api.env.host_string = n
            api.run("chmod -R a+x {}bin/".format(self.khome))

    # Setup and push Zookeeper config files to broker nodes
        log.info("Setup ZooKeeper Configuration files on broker nodes")
        zk = ZKConfigClass.ZK(self.configuration['KAFKA']['zkdatadir'], self.bnodes, self.configuration['KAFKA']['zkport'])
        zookeeperProperties = open("./clusterkafka/templates/zookeeper.properties", 'w')
        zookeeperProperties.write(self.render.render(zk))
        zookeeperProperties.close()

        for n in self.bnodes:
            api.env.host_string = n
            api.put("./clusterkafka/templates/zookeeper.properties",
                    self.khome + "config/zookeeper.properties")

    # Create and push unique myid file
        log.info("Creating broker node zookeeper id files")
        for n,id in zip(self.bnodes,range(len(self.bnodes))):
            api.env.host_string = n
            api.run("echo {} > {}myid".format(str(id),self.configuration['KAFKA']['zkdatadir']))

    # Setup and distribute unique Kafka config files
        log.info("Creating and transferring Kafka configuration files.")
        for n,id in zip(self.bnodes,range(len(self.bnodes))):
            api.env.host_string = n
            ks = KSConfigClass.KS(id , n, self.configuration['KAFKA']['logsdir'], self.bnodes,
                                  self.configuration['KAFKA']['zkport'], self.configuration['KAFKA']['sport'])

            kafkaserverProperties = open("./clusterkafka/templates/kafkaserver.properties", 'w')
            kafkaserverProperties.write(self.render.render(ks))
            kafkaserverProperties.close()
            api.put("./clusterkafka/templates/kafkaserver.properties", self.khome + "config/server.properties")


