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

logger = logging.getLogger('fabricsoodt.test')
fabric.api.settings.warn_only = True
render = pystache.Renderer()


def createDataFile(of,bs,count):
    """ Creates an input file """
    logger.info("Creating an input file of {0} bytes in {1}".format(bs*count,of))
    ret = fabric.api.run("dd if=/dev/urandom of=/tmp/input.dat bs=1000000 count=1000")
    if ret.failed and not fabric.contrib.console.confirm("Failed to create data file, continue anyways?"):
        fabric.utils.abort("Aborting at user request")

def ProducerGo(propertiesFile):
    """ Starts a producer """
    logger.info("Starting producer with" + propertiesFile)
    ret = fabric.api.run("screen -d -m java -cp $HTK_HOME/build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTConsumer.HTConsumer {0} sleep 1; touch poodles".format(propertiesFile))
    #ret = fabric.api.run("java -cp $HTK_HOME/build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTProducer.HTProducer {0} sleep 1; touch poodles".format(propertiesFile))
    if ret.failed and not fabric.contrib.console.confirm("Failed start producer, continue anyways?"):
        fabric.utils.abort("Aborting at user request")


def initProducer(topic, partitionCount,zocP,msgsize,ID):
    """ Create a producer properties file, carried out on local node."""
    myclass = fabricsoodt.ProducerConfigClass.PC(topic,partitionCount,zocP,msgsize,ID)
    Properties = open("./fabricsoodt/templates/producer{}.properties".format(ID), 'w')
    Properties.write(render.render(myclass))
    Properties.close()

def ConsumerGo(propertiesFile):
    """ Starts a multithreaded consumer """

    logger.info("Starting consumer with" + propertiesFile)
    ret = fabric.api.run("screen -d -m java -cp $HTK_HOME/build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTConsumer.HTConsumer {0} sleep 1; touch poodles".format(propertiesFile))
    #ret = fabric.api.run("java -cp $HTK_HOME/build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTConsumer.HTConsumer {0} sleep 1; touch poodles".format(propertiesFile))
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
    ret = fabric.api.run("screen -d -m $K_HOME/bin/kafka-topics.sh --create --zookeeper {0} --replication-factor {1} --partition {2} --topic {3}; sleep 1".format(zoo, replicationFactor, partitions, topic))
    logger.info("Starting topic: --zookeeper {0} --replication-factor {1} --partition {2} --topic {3}; sleep 1".format(zoo, replicationFactor, partitions, topic))
    logger.info("Status of ret{}".format(ret))
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


# Start consumers
def startupConsumers(config):
    '''
    Implements option selected:
    NOTE1: 1 Group only and no replication only - for max performance - future work - impact of more groups and replication for greater reliability...
    NOTE2: No Consumer Pipes == NoTopics*NoPartitions.
        Having more nodes than Consumer Pipes is nonsensical.
        Having exactly as many nodes as consumer pipes will result in exactl 1 consumer instance per node
        Having less nodes than Consumer Pipes means either multi threading or multple consumer instance/node will be used based on decision

    Option1: NoTopics > NoNodes: NoTopics/NoNodes = ConPerNode
	    SubOption1a: Consumers implemented as multiple threads or separate instances
    Option2: NoNodes > NoTopics: NoNodes/NoTopics = ProdsPerTopic
	    Suboption2a: Consumers implemented as multiple threads or separate instances
    Addional variables: NoPartitions, NoBrokerNodes,
    '''

    # Create python lists of relevant lists in config file
    TOPICS = map(lambda x: x.strip(" "), config['TEST']['topics'].split(","))
    GIDS = map(lambda x: x.strip(" "), config['CONSUMERS']['groups'].split(","))
    CNODES = map(lambda x: x.strip(" "), config['CONSUMERS']['nodes'].split(","))
    BNODES = map(lambda x: x.strip(" "), config['KAFKA']['nodes'].split(","))

    ConsumerPipes = len(TOPICS)*config['TEST']['partitions']    #Total number of consumer instances whether that is indepenant processes or threads
    ConsumerPipesPerNode=ConsumerPipes/len(CNODES)

    logger.info("No consumer pipes: "+str(ConsumerPipes))
    logger.info("No topics: "+str(len(TOPICS)))
    logger.info("No nodes: "+str(len(CNODES)))
    logger.info("No partitions: "+ config['CONSUMERS']['partitions'])
    logger.info("Consumer pipes per node: " +str(ConsumerPipesPerNode))

    if (ConsumerPipes < len(CNODES)): #Sanity checks
        if not fabric.contrib.console.confim("Poor match of consumer node numbers to topics and partitions, suggest reconsidering test structure.  Continue anyways?"):
            fabric.utils.abort("Aborting at user request")
    elif (ConsumerPipes%len(CNODES)!=0): #Bad selection
        if not fabric.contrib.console.confim("Poor match of consumer node numbers to topics and partitions, suggest reconsidering test structure.  Continue anyways?"):
            fabric.utils.abort("Aborting at user request")


    if (len(TOPICS)>=len(CNODES)): #NoTopics>NoNodes
    #Option1: NoTopics > NoNodes: NoTopics/NoNodes = ConPerNode
        #SubOption1a: Consumers implemented as multiple threads or separate instances
        logger.info("Testing option1: NoTopics>=NoNodes")
        logger.info("The number of threads * number of consumer instances must equal {} ConsumerPipesPerNode"._format(ConsumerPipesPerNode))
        logger.info("Threads in the same consumer will consume from the same topic")
        logger.info("The same CNODE can host pipes reading from a common topic (multiple threads from within a common consumer) from multiple partitions, or separte consumers instances can be part of the same group but be on diff nodesspread accross different nodes but still read from the same topic")
        Alternate=raw_input(Alternate y/n?)

        if
        NoThreadsPerConsumer=
        NoConsumersPerNode=raw_input("Based on above how many consumer instance should a node get?  ")
        logger.info(
        "Starting a consumer using option1: 1 consumer instance ID{0}, with {1} threads, all in the same group {2}, following the same topic {3}".format(
            "001", config['CONSUMERS']['threadsperc'], GIDS[0], TOPICS[0], ))
        node =0
        for (t in TOPICS):
            if Alternate=='n':
                for (c in range(NoConsumersPerNode)):

                    # Create properties file
                    CID=t+str(c)    #ConsumerID = Thread+ConsumeronNodeNumber
                    fabric.api.execute(initConsumer, config['TEST']['zocC'], GIDS[0], t,NoThreadsPerConsumer,CID, hosts=['localhost'])
                    #remotePropsFile = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/"
                    remotePropsFile = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/Consumer{0}.properties".format(CID)
                    logger.info("Creating consumer{0} with {1} threads, pulling from topic: {2}, and running on node{3}".format(CID,NoThreadsPerConsumer,t,CNODES[n]))

                    # Transfer properties file
                    logger.info("Transfer consumer{0}.properties".format(CID))
                    fabric.api.execute(fabricsoodt.distribute.copy,"./fabricsoodt/templates/consumer{0}.properties".format(CID), remotePropsFile, hosts=CNODES[node])

            elif Alternate =='y':
            else:
                fabric.utils.abort("Bad entry aborting")

        # Start consumer
        fabric.api.execute(ConsumerGo, remotePropsFile, hosts=[CNODES[0]])


    elif (len(TOPICS)<len(CNODES)): #NoTopics < NoNodes
    #Option2: NoNodes > NoTopics: NoNodes/NoTopics = ProdsPerTopic
        #Suboption2a: Consumers implemented as multiple threads or separate instances
    logger.info("Testing option1: NoTopics<NoNodes")







    elif option == 2:
        logger.info("StartupConsumers() option not implemented")
    else:
        logger.warn("No valid StartupConsumers() option selected")


# Start producer
def startupProducers(config):
    """
    Implements option selected:
    FOR All CASES: No topics * No partions per topic = No Consumer threads total (i.e No Consumers*No thredas per consumer)
    Option1: NoTopics > NoNodes: NoTopics/NoNodes = ProdsPerNode
    Option2: NoNodes > NoTopics: NoNodes/NoTopics = ProdsPerTopic
    ;Further variables: No Partitions, No Broker nodes
    """
    # Create python lists of relevant lists in config file
    TOPICS = map(lambda x: x.strip(" "), config['TEST']['topics'].split(","))
    GIDS = map(lambda x: x.strip(" "), config['CONSUMERS']['groups'].split(","))
    PNODES = map(lambda x: x.strip(" "), config['PRODUCERS']['nodes'].split(","))


    if len(TOPICS)>=len(PNODES):
    #Option1: NoTopics > NoNodes: NoTopics/NoNodes = ProdsPerNode
        ProdsPerNode = len(TOPICS)/len(PNODES)
        logger.infor("Starting producers")
        logger.info("NoProducers {}".format(len(TOPICS)))
        logger.info("NoProducersPerNode {}".format(ProdsPerNode))

        ids=range(len(TOPICS))
        node=0
        loopCount=0
        for (t,id) in zip(TOPICS,ids):

            if (node == len(PNODES)):
                node=0
                loopCount=loopCount+1

            # Create properties file
            fabric.api.execute(initProducer, t, config['CONSUMERS']['threadsperc'],config['TEST']['zocP'],config['TEST']['msgsize'],id,hosts=['localhost'])
            remotePropsFile = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/"

            # Transfer properties file
            logger.info("Transfer producer{}.properties".format(id))
            fabric.api.execute(fabricsoodt.distribute.copy,"./fabricsoodt/templates/producer{}.properties".format(id), remotePropsFile,hosts=PNODES[node+loopCount])

            # Start Producers
            fabric.api.execute(ProducerGo, remotePropsFile, hosts=[PNODES[node+loopCount]])
            node =node+1
    elif (len(PNODES)>len(TOPICS)):
    #Option2: NoNodes > NoTopics: NoNodes/NoTopics = ProdsPerTopic
        ProdsPerTopic = len(PNODES)/len(TOPICS)
        logger.infor("Starting producers")
        logger.info("NoProducers {}".format(len(PNODES)))
        logger.info("NoProducersPerNode {}".format(ProdsPerTopic))

        ids=range(len(PNODES))
        topic=0
        for (n,id) in zip(PNODES,ids):

            if (topic == len(TOPICS)):
                topic=0

            # Create properties file
            fabric.api.execute(initProducer, TOPICSC[topic], config['CONSUMERS']['threadsperc'],config['TEST']['zocP'],config['TEST']['msgsize'],id,hosts=['localhost'])
            remotePropsFile = config['CONFIGS']['destination'] + "HTKafka-benchmark/build/resources/main/"

            # Transfer properties file
            logger.info("Transfer producer{}.properties".format(id))
            fabric.api.execute(fabricsoodt.distribute.copy,"./fabricsoodt/templates/producer{}.properties".format(id), remotePropsFile,hosts=PNODES[n])

            # Start Producers
            fabric.api.execute(ProducerGo, remotePropsFile, hosts=[PNODES[n]])
            topic=topic+1
    else:
        logger.warn("No valid startup Producer configuration")

# Collate results

# Producer report

# shutdown consumers

#Create Data
def createData(of,bs,count,NoFiles,PNODES):
    for i in range(NoFiles):
        of=of+"input{}.dat".format(i)
        fabric.api.execute(createData,of,bs,count,hosts=[PNODES])
