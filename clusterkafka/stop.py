""" Module of functions for stopping components """

import logging
from yarn import api

log = logging.getLogger(__name__)

@api.parallel
def stop_zserver(k_home):
    """ Stop kafka included zookeeper server on api.env.hots_string node """

    #TODO does this work without screen
    api.run("screen -dm bash -c \"{}bin/zookeeper-server-stop.sh {}config/zookeeper.properties; exec bash\"".format(k_home,k_home))
    log.info("Stopped zookeeper server on {}".format(api.env.host_string))


@api.parallel
def stop_kserver(k_home):
    """ Stop a kafka broker server on api.env.hots_string node """

    #TODO does this work without screen
    api.run("screen -dm bash -c \"{}bin/zookeeper-server-start.sh {}config/zookeeper.properties; exec bash\"".format(k_home,k_home))
    api.run("screen -dm bash -c \"{}bin/kafka-server-stop.sh {}config/server.properties; exec bash\"".format(k_home,k_home))
    log.info("Stopped Kafka server on {}".format(api.env.host_string))
