""" Module of functions for starting components """

import logging
from yarn import api

log = logging.getLogger(__name__)

@api.parallel
def start_zserver(k_home):
    """ Start kafka included zookeeper server on api.env.hots_string node inside a screen session """

    api.run("screen -dm bash -c \"{}bin/zookeeper-server-start.sh {}config/zookeeper.properties; exec bash\"".format(k_home,k_home))
    log.info("Started zookeeper server on {}".format(api.env.host_string))


@api.parallel
def start_kserver(k_home):
    """ Start a kafka broker srver on api.env.hots_string node inside a screen session """

    api.run("screen -dm bash -c \"{}bin/kafka-server-start.sh {}config/server.properties; exec bash\"".format(k_home,k_home))
    log.info("Started Kafka server on {}".format(api.env.host_string))

