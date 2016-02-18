''' Module containing clean(), install(). startBrokers(), stopBrokers(), runTest() '''
# External imports
import logging
import sys
import fabric.utils
import fabric.api
import fabric.contrib.files
import fabric.contrib.console
import time

# Package imports
import fabricsoodt
import fabricsoodt.setup
import fabricsoodt.build
import fabricsoodt.distribute
import fabricsoodt.start
import fabricsoodt.stop
import fabricsoodt.test

logger = logging.getLogger('fabricsoodt.operate')
logging.getLogger("paramiko").setLevel(logging.WARNING)


def cleanBroker(directory):
	''' Removes contents of a directory '''
	ret=fabric.api.run("rm -r {0}".format(directory))
	if ret.failed:
		logger.error("Failed to cleann broker node directory: {0}".format(directory))


def clean():
    # Returns the contents of provided ini file as a dictionary of dictionaries
    fabric.api.local("rm -r ./logs/*")
    fabric.api.local("rm -r ./fabricsoodt/templates/myid")
    fabric.api.local("rm -r ./fabricsoodt/templates/*.properties")
#    fabric.api.local("rm -r ./downloads/*")


def install():
    # INITIALISATION
    # ------------------------------------------

    # Returns the contents of provided ini file as a dictionary of dictionaries
    configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
    logger.info("\n> install() Reading configuration file: {0}".format(sys.argv[2]))

    # Setup Fabric fabric.api.envs
    fabric.api.env.colorize_error = True
    fabric.api.env.user = configuration['CONFIGS']['user']
    fabric.api.env.roledefs = {'login': ['localhost']}
    logger.info("Set up fabric envs")

    allIPs = set()
    nodeIPs = map(lambda z: z.strip(" "), configuration['KAFKA']['nodes'].split(","))
    fabric.api.env.roledefs['KAFKAnodeIPs'] = nodeIPs
    configuration['KAFKA']['NODES'] = nodeIPs  # Create an entry containing the IPs in a python list
    [allIPs.add(x) for x in nodeIPs]  # Create an ALLnodesIPs list
    fabric.api.env.roledefs['ALLnodesIPs'] = allIPs


    # Set local env variables:
    fabric.api.execute(fabricsoodt.setup.setEnvs, configuration['CONFIGS']['envvars'], configuration['CONFIGS']['user'],roles=['login'])
    logger.info("Set envars on broker machines")

    # DOWNLOADS
    pwd = fabric.api.local("pwd", capture=True)
    downloads = pwd + "/downloads/"

    dList = []

    if fabric.api.execute(fabricsoodt.setup.download, configuration['KAFKA']['url'], downloads, roles=['login']):
        logger.info("\n> Downloaded: {0} to {1}".format('KAFKA', downloads))

    else:
        logger.error("\n> Failed to download{0}".format('KAFKA'))
        if not fabric.contrib.console.confirm("Do you wish to continue with out installing {0}".format('KAFKA')):
            fabric.api.abort("Aborting at users request")
        else:
            logger.info("\n> Continuing without {0}".format('KAFKA'))
            dList.appen('KAFKA')

    for i in dList: del configuration[i]

    # EXTRACT and BUILD
    # ------------------------------------------

    tarPath = downloads + configuration['KAFKA']['url'].split('/')[-1:][0]
    # Extract returns the resulting folder name, capture to configuration
    configuration['KAFKA']['folderName'] = fabricsoodt.build.extract(tarPath, downloads)
    # Set Application HOME path
    configuration['KAFKA']['HOME'] = configuration['CONFIGS']['destination'] + configuration['KAFKA']['folderName']

    if configuration['KAFKA']['build']:
        # Setup required environment variables
        fabric.api.execute(fabricsoodt.setup.setEnvs, configuration['KAFKA']['envvars'],
                           configuration['CONFIGS']['user'], roles=['login'])

        # Run build
        msg = fabricsoodt.build.build('KAFKA', tarPath, configuration['CONFIGS']['sudo'],
                                      configuration['KAFKA']['test'])
        logger.info(msg)
    else:
        logger.info("\n> End build stage")

    # DISTRIBUTE and CONFIGURE
    # ------------------------------------------

    ###################Commented out for future use ##################

    # Dependancies are currently ensured/checked for in 3 manners:
    # - The readme requires basics for install
    # - If a build is requested that build function ensures that application's dependancies are met
    # - This currently commented out section provides a means for additional packages to be installed, for instance a need for svn is indicated but currently unused.  To activate uncomment the following lines and provide the apt-get / yum recognisable names of dependancies in list ToQuery


    #	ToQuery = [['subversion',1]]
    #	missing = execute(distribute.dependanciesCheck,ToQuery,setup.getos(),roles=['ALLnodesIPs'])
    #	if filter(lambda d: d !="",missing.values()):
    #		logger.error("The following dependancies are missing on the deploy nodes, please install and re-run: {}".format(str(missing)))
    #		fabric.utils.abort("Aborting due to missing dependancies on remote nodes")

    ###################################################################

    # Check ulimit -u is above 4096
    #fabric.api.execute(fabricsoodt.distribute.ulimitCheck, roles=['ALLnodesIPs'])
    # Check fabric.api.env variables are set:
    # fabric.api.execute(fabricsoodt.distribute.variablesCheck,roles=['ALLnodesIPs'])
    # Check destination folder exists
    destination = configuration['CONFIGS']['destination']
    fabric.api.execute(fabricsoodt.distribute.existsCreate, destination, roles=['ALLnodesIPs'])


    # Distribute application
    source = downloads + configuration['KAFKA']['folderName']
    logger.info("\n> Transfering {0} to nodes: {1}".format(source, destination))
    fabric.api.execute(fabricsoodt.distribute.transfer, source, destination, roles=["KAFKAnodeIPs"])

    # Distribute configuration files
    for server in range(1, len(configuration['KAFKA']['NODES']) + 1):
        fabric.api.execute(fabricsoodt.distribute.configKafka, configuration['KAFKA'],server, hosts=nodeIPs[server-1])

    logger.info("\n> Completed {} configuration".format('KAFKA'))


    # Start up instalation
    # Test installation

    logger.info("\n> SOODT deployment completed")
    logger.info("\n ############# END ##############")


def startBrokers():
    ''' Start components based on config file requests and logical order '''
    # Returns the contents of provided ini file as a dictionary of dictionaries
    configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
    logger.info("\n> StartBrokers() Reading configuration file: {0}".format(sys.argv[2]))

    fabric.api.env.colorize_error = True
    fabric.api.env.user = configuration['CONFIGS']['user']



    # Create an entry in configuration libraries containing app specific IPs in a python list
    nodeIPs = map(lambda z: z.strip(" "), configuration['KAFKA']['nodes'].split(","))
    configuration['KAFKA']['NODES'] = nodeIPs
    logger.info(">Captured node IPs in config: "+str(nodeIPs))

    logger.info("Cleaning broker cluster")
    fabric.api.execute(cleanBroker,configuration['KAFKA']['logsdir'],hosts=nodeIPs)
    fabric.api.execute(cleanBroker,configuration['KAFKA']['zkdatadir'],hosts=nodeIPs)

    #Cannot implement below without restructuring as configKafka relies on config['KAFKA']['HOME'] being set which is only done in operate.install()....
    #logger.info("\n>Refresh kafka configuration")
    #fabric.api.execute(fabricsoodt.distribute.configKafka, configuration['KAFKA'],hosts=nodeIPs)

    logger.info("\n> Starting Kafka")
    fabricsoodt.start.startupKafka(configuration['KAFKA'])


# if configuration['MESOS']['start']:
#		logger.info("\n> Starting Mesos")
#		fabricsoodt.start.startupMesos(configuration['MESOS'])

def stopBrokers():
    """ Stop components based on config file requests and logical order """
    fabric.api.env.colorize_error = True
    # Returns the contents of provided ini file as a dictionary of dictionaries
    configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
    fabric.api.env.user = configuration['CONFIGS']['user']
    logger.info("\n> stopBrokers() Reading configuration file: {0}".format(sys.argv[2]))
    # Create an entry in configuration libraries contaning app specific IPs in a python list
    nodeIPs = map(lambda z: z.strip(" "), configuration['KAFKA']['nodes'].split(","))
    configuration['KAFKA']['NODES'] = nodeIPs

    logger.info("\n> Stopping Kafka")
    fabricsoodt.stop.stopKafka(configuration['KAFKA'])


#	if not configuration['MESOS']['start']:
#		logger.info("\n> Stopping Mesos")
#		fabricsoodt.stop.stopMesos(configuration['MESOS'])

def runTest():
    """ Initialises producer and consumer nodes and starts up first the producers and then the conumers """

    # Get config
    # Returns the contents of provided ini file as a dictionary of dictionaries
    configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
    logger.info("\n> StartProducers() Reading configuration file: {0}".format(sys.argv[2]))
    pwd = fabric.api.local("pwd", capture=True)
    downloads = pwd + "/downloads/"
    logger.info(">>Set downloads to{}".format(downloads))

    # Setup Fabric fabric.api.envs
    fabric.api.env.colorize_error = True
    fabric.api.env.user = configuration['CONFIGS']['user']

    # Extract list of consumer node ips into python list
    BrokerIPs = map(lambda x: x.strip(" "), configuration['KAFKA']['nodes'].split(","))
    ConsumerIPs = map(lambda x: x.strip(" "), configuration['CONSUMERS']['nodes'].split(","))
    ProducerIPs = map(lambda x: x.strip(" "), configuration['PRODUCERS']['nodes'].split(","))
    fabric.api.env.roledefs['brokers'] = BrokerIPs
    fabric.api.env.roledefs['consumers'] = ConsumerIPs
    fabric.api.env.roledefs['producers'] = ProducerIPs

    logger.info("This is roledefs " + str(fabric.api.env.roledefs))


    # Create zookeeper connect ip:port list
    zocC = ""
    zocP = ""
    for ip in BrokerIPs:
        zocC = zocC + str(ip) + ":" + str(configuration['KAFKA']['zkport']) + ","
        zocP = zocP + str(ip) + ":" + str(configuration['KAFKA']['sport']) + ","
    zocC = zocC[:-1]
    zocP = zocP[:-1]
    configuration['TEST']['zocC'] = zocC
    configuration['TEST']['zocP'] = zocP

    logger.info("Constructed zookeeper is: {0}".format(zocC))

    # Set $HTK_HOME remotely
    logger.info("\n----------------Setting HTK_HOME----------------")
    fabric.api.execute(fabricsoodt.distribute.remoteSetVar,"HTK_HOME=" + configuration['CONFIGS']['destination'] + "HTKafka-benchmark/",roles=['consumers'])
    fabric.api.execute(fabricsoodt.distribute.remoteSetVar,"HTK_HOME=" + configuration['CONFIGS']['destination'] + "HTKafka-benchmark/",roles=['producers'])
    logger.info("\n> Set HTK_HOME ={0}".format(configuration['CONFIGS']['destination'] + "HTKafka-benchmark/"))

    logger.info("\n----------------Setting K_HOME----------------")
    fabric.api.execute(fabricsoodt.distribute.remoteSetVar,"K_HOME=/home/jwyngaard/.local/kafka_2.10-0.8.2.1",roles=['consumers'])
    fabric.api.execute(fabricsoodt.distribute.remoteSetVar,"K_HOME=/home/jwyngaard/.local/kafka_2.10-0.8.2.1",roles=['producers'])
    logger.info("\n> Set K_HOME ={0}".format(configuration['CONFIGS']['destination'] + "Kafka-benchmark/"))

    # Distribute HT-kafka-benchmark to Consumer and producer nodes
    logger.info("\n----------------Distributing HTKafka-benchmark----------------")
    HTKsrc = downloads + "build"
    HTKdest = configuration['CONFIGS']['destination']+"HTKafka-benchmark/"
    logger.info("Transfering {0} to {1}".format(HTKsrc, HTKdest))
    fabric.api.execute(fabricsoodt.distribute.copy, HTKsrc, HTKdest, roles=['consumers'])
    fabric.api.execute(fabricsoodt.distribute.copy, HTKsrc, HTKdest, roles=['producers'])

    #Run a test
    fabricsoodt.test.GoTest1(configuration)
    print ("FINISHED TEST")
#---------------------- ---------------------
#    # Start topic(s)
#    logger.info("\n----------------Starting topics----------------")
#    fabricsoodt.test.startupTopics(configuration['TEST'])
#
#    #	#Start producers=(s)
#    logger.info("\n----------------Starting producers----------------")
#    fabricsoodt.test.startupProducers(configuration)
#
#    time.sleep(10)
#    # Start consumer(s)
#    logger.info("\n---------------- Starting consumers----------------")
#    fabricsoodt.test.startupConsumers(configuration)

#
#	#Collate results()
#	#Produce report()
#	#Delete topics()
#   #Shutdown Consumers()

def createData():
    configuration = fabricsoodt.setup.readconfig(str(sys.argv[2]))
    logger.info("\n---------------- Creating data on producer nodes ----------------")
    logger.info("\n> createData() reading configuration file: {0}".format(sys.argv[2]))
    of=configuration['TEST']['of']
    bs=configuration['TEST']['bs']
    count=configuration['TEST']['count']
    NoFiles=configuration['TEST']['NoFiles']
    PNODES = map(lambda z: z.strip(" "), configuration['PRODUCERS']['nodes'].split(","))
    fabricsoodt.test.createData(of,bs,count,NoFiles,PNODES)


