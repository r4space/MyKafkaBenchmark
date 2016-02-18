''' Module of funtions for stopping components '''

import logging
import fabric.api 

logger=logging.getLogger('fabricsoodt.stop')
fabric.api.settings.warn_only=True

##KAFKA FUNCTIONS
def stopZookeeper():
	''' Zookeeper stop function '''

	ret = fabric.api.run("screen -d -m $K_HOME/bin/zookeeper-server-stop.sh $K_HOME/config/zookeeper.properties; sleep 1")
	
	if ret.failed:
		logger.error("Failed to stop Zookeeper node")
	else:
		logger.info("Stopped zookeeper node")
def stopKCluster ():
	''' Kafka Broker Cluster stop function '''
	
	ret = fabric.api.run("screen -d -m $K_HOME/bin/kafka-server-stop.sh $K_HOME/config/server.properties; sleep 1")
	
	if ret.failed:
		logger.error("Failed to stop Kafka Broker Cluster Node")
	else:
		logger.info("Stopped Kafka broker cluster node")

def stopKafka(config):
	''' Stops a kafka cluster '''
	
	logger.info("Stopping kafka cluster")
	fabric.api.execute(stopKCluster,hosts=config['NODES'])
	
	logger.info("Stopping zookeeper cluster")
	fabric.api.execute(stopZookeeper,hosts=config['NODES'])
