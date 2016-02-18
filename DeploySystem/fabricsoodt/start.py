''' Module of funtions for starting components '''

import logging
import fabric.api 

logger=logging.getLogger('fabricsoodt.start')
fabric.api.settings.warn_only=True


##KAFKA FUNCTIONS
def startupZookeeper():
	''' Zookeeper startup function '''

	ret = fabric.api.run("screen -d -m $K_HOME/bin/zookeeper-server-start.sh $K_HOME/config/zookeeper.properties; sleep 1")
	
	if ret.failed:
		logger.error("Failed to start Zookeeper node")
	else:
		logger.info("Started zookeeper node")

def startupKCluster ():
	''' Kafka Broker Cluster startup function '''
	
	ret = fabric.api.run("screen -d -m $K_HOME/bin/kafka-server-start.sh $K_HOME/config/server.properties; sleep 1")
	
	if ret.failed:
		logger.error("Failed to start Kafka Broker Cluster Node")
	else:
		logger.info("Started Kafka broker cluster node")



def startupKafka(config):
	''' Starts a kafka cluster '''
#	logger.info("Cleaning broker cluster")
#	fabric.api.execute(cleanBroker,config['logsdir'],hosts=config['NODES'])
#	fabric.api.execute(cleanBroker,config['zkdatadir'],hosts=config['NODES'])
	
	logger.info("Starting zookeeper cluster")
	fabric.api.execute(startupZookeeper,hosts=config['NODES'])

	logger.info("Starting kafka cluster")
	fabric.api.execute(startupKCluster,hosts=config['NODES'])




























