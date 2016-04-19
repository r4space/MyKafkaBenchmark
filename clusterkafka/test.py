''' Module containing methods for executng kafka tests '''
import logging
import fabric.api
import fabric.utils
import fabric.contrib.files
import fabric.contrib.console
import fabricsoodt.start
import pystache
import fabricsoodt.ProducerConfigClass
import fabricsoodt.ConsumerConfigClass
import time

logger = logging.getLogger('fabricsoodt.test')
fabric.api.settings.warn_only = True
render = pystache.Renderer()


def createDataFile(of,bs,count):
    """ Creates an input file """
    logger.info("Creating an input file of {0} bytes in {1}".format(bs*count,of))
    ret = fabric.api.run("dd if=/dev/urandom of={0} bs={1} count={2}".format(of,bs,count))
    if ret.failed and not fabric.contrib.console.confirm("Failed to create data file, continue anyways?"):
        fabric.utils.abort("Aborting at user request")

#Create Data
def createData(of,bs,count,NoFiles,PNODES):
    for i in range(NoFiles):
        of=of+"input{}.dat".format(i)
        fabric.api.execute(createDataFile,of,bs,count,hosts=PNODES)

def ProducerGo(propertiesFile):
    """ Starts a producer """
    logger.info("Starting producer with" + propertiesFile)
    ret = fabric.api.run("java -cp $HTK_HOME/build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTProducer.HTProducer {0} ; sleep 1; touch frodo".format(propertiesFile))
    if ret.failed and not fabric.contrib.console.confirm("Failed start producer, continue anyways?"):
        fabric.utils.abort("Aborting at user request")


def initProducer(topic, partitionCount,zP,msgsize,ID,inputFile):
    """ Create a producer properties file, carried out on local node."""
    myclass = fabricsoodt.ProducerConfigClass.PC(topic,partitionCount,zP,msgsize,ID,inputFile)
    Properties = open("./fabricsoodt/templates/producer{}.properties".format(ID), 'w')
    Properties.write(render.render(myclass))
    Properties.close()

def ConsumerGo(propertiesFile):
    """ Starts a multithreaded consumer """

    logger.info("Starting consumer with" + propertiesFile)
#    ret = fabric.api.run("screen -d -m java -cp $HTK_HOME/build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTConsumer.HTConsumer {0}".format(propertiesFile))
    ret = fabric.api.run("java -cp $HTK_HOME/build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTConsumer.HTConsumer {0}".format(propertiesFile))
    if ret.failed and not fabric.contrib.console.confirm("Failed start consumer, continue anyways?"):
        fabric.utils.abort("Aborting at user request")


def initConsumer(zocC, groupID, topic, partitionCount, ID):
    """ Create a consumer properties file, carried out on local node."""
    myclass = fabricsoodt.ConsumerConfigClass.CC(zocC, groupID, topic, partitionCount, ID)
    Properties = open("./fabricsoodt/templates/consumer{0}.properties".format(ID), 'w')
    Properties.write(render.render(myclass))
    Properties.close()


# Call java functions
def initTopic(topic, partitions, replicationFactor, zoo):
    """ Start topic on broker cluter """
    logger.info("Starting topic: --zookeeper {0} --replication-factor {1} --partition {2} --topic {3}; sleep 1".format(zoo, replicationFactor, partitions, topic))
    ret = fabric.api.run("$K_HOME/bin/kafka-topics.sh --create --zookeeper {0} --replication-factor {1} --partition {2} --topic {3}".format(zoo, replicationFactor, partitions, topic))
    if ret.failed:
        logger.error("Failed to create topic {0} on broker cluster {1}".format(topic, zoo))
    else:
        logging.info(
            'Sucessfully started topic: {0}, partions: {1}, replication-factor{2} on brokercluster: {3}'.format(topic,
                                                                                                                partitions,
                                                                                                                replicationFactor,
                                                                                                                zoo))
# Kafka test functions
def startupTopics(configT):
    """ Starts topics on broker cluster """
    TOPICS = map(lambda x: x.strip(" "), configT['topics'].split(","))
    p = configT['partitions']
    r = configT['replications']
    z = configT['zocC']
    # aBroker="'"+z.split(",")[0].split(":")[0]+"'"
    aBroker = z.split(",")[0].split(":")[0]

    for t in TOPICS:
        fabric.api.execute(initTopic, t, p, r, z, hosts=[aBroker])


def GoTest1(config):
    TOPICS = map(lambda x: x.strip(" "), config['TEST']['topics'].split(","))
    GIDS = map(lambda x: x.strip(" "), config['CONSUMERS']['groups'].split(","))
    PNODES = map(lambda x: x.strip(" "), config['PRODUCERS']['nodes'].split(","))
    CNODES = map(lambda x: x.strip(" "), config['CONSUMERS']['nodes'].split(","))
    BNODES = map(lambda x: x.strip(" "), config['KAFKA']['nodes'].split(","))
    p = config['TEST']['partitions']
    r = config['TEST']['replications']
    zC = config['TEST']['zocC']
    zP = config['TEST']['zocP']
    th = config['TEST']['threadsperconsumer']
    msgsize=config['TEST']['msgsize']
    aBroker = PNODES[0]

    #Create data:
    logger.info("\n----------------Creating Data----------------")
    createData(config['TEST']['of'],config['TEST']['bs'],config['TEST']['count'],config['TEST']['nofiles'],PNODES)

   # Start topic(s)
    logger.info("\n----------------Starting topics----------------")
    logger.info("Starting topic{0} on broker: {1}".format(TOPICS[0],aBroker))
    fabric.api.execute(initTopic, TOPICS[0], p, r, zC, hosts=[aBroker])
    time.sleep(5)

    # Create producer properties file
    pid=001
    logger.info("\n----------------Create producer properties files----------------")
    fabric.api.execute(initProducer, TOPICS[0],p,zP,msgsize,pid,"input0.dat",hosts=['localhost'])

    # Transfer properties file
    logger.info("\n----------------Transfer producer properties files----------------")
    dest = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/"
    logger.info("Copy ./fabricsoodt/templates/producer{0}.properties to {1}".format(pid, dest))
    fabric.api.execute(fabricsoodt.distribute.copy,"./fabricsoodt/templates/producer{}.properties".format(pid), dest,hosts=PNODES[0])

    # Start Producers
    remotePropsFile = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/producer{}.properties".format(pid)
    logger.info("\n----------------Start producer----------------")
    fabric.api.execute(ProducerGo, remotePropsFile, hosts=[PNODES[0]])

    time.sleep(5)

   # Create consumer properties file
    cid=001
    logger.info("\n----------------Create consumer properties files----------------")
    fabric.api.execute(initConsumer, zC, GIDS[0], TOPICS[0],th,cid, hosts=['localhost'])
    #remotePropsFile = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/Consumer{0}.properties".format(CID)

    # Transfer properties file
    logger.info("\n----------------Transfer consumer properties files----------------")
    dest = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/"
    logger.info("Copy ./fabricsoodt/templates/consumer{0}.properties to {1}".format(cid, dest))
    fabric.api.execute(fabricsoodt.distribute.copy,"./fabricsoodt/templates/consumer{0}.properties".format(cid), dest, hosts=CNODES[0])

   # Start consumer
    remotePropsFile = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/consumer{}.properties".format(cid)
    logger.info("\n----------------iStart Consumer ----------------")
    fabric.api.execute(ConsumerGo, remotePropsFile, hosts=[CNODES[0]])
