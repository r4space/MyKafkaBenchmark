from __future__ import with_statement

''' Module containing function for the distribution of SOODT components and configurations '''

import logging
# Pystache Config file classes
import pystache
import fabricsoodt.ZKConfigClass
import fabricsoodt.KSConfigClass

# import fabric.api
import fabric.api
import fabric.utils
import fabric.contrib.files

fabric.api.settings.warn_only = True

logger = logging.getLogger('fabricsoodt.distribute')

# Prepare pystache renderer
render = pystache.Renderer()


def chmodFolder(folder, options):
    ''' Run chmod with options '''
    ret = fabric.api.run("chmod {0} {1}".format(options, folder))
    if ret.failed and not fabric.contrib.console.confirm("Something went wrong, continue anyways? "):
        fabric.utils.abort("Aborting at user request")


def existsCreate(path):
    ''' Checks if a path exists and if not creats it '''
    if not fabric.contrib.files.exists(path):
        ret = fabric.api.run("mkdir -p " + path)
        if ret.failed and not fabric.contrib.console.confirm(
                                "Failed to make directory " + path + ", continue anyways?"):
            fabric.utils.abort("Aborting at user request")


def ulimitCheck():
    ''' Check ulimit on all nodes and raises it if necessary '''
    ret = fabric.api.run("ulimit -u")

    if int(ret) < 4095:
        ret = fabric.api.run("ulimit -Su 4095")

    if ret.failed and not fabric.contrib.console.confirm("Failed, continue anyways?"):
        fabric.utils.abort("Aborting at user request")


def copy(src, dest):
    """Simple copy """

    logger.info("Doing simple copy of {0} to {1}".format(src, dest))
    if not fabric.contrib.files.exists(dest):
        ret = fabric.api.run("mkdir -p " + dest)
        if ret.failed:
            fabric.utils.abort(
                "Failed to make directory {0}, likely due to permissions issue, aborting".format(dest))
    elif not fabric.contrib.console.confirm("{} already found, re-copy it?".format(src)):
        return 1

    ret = fabric.api.put(src, dest, use_sudo=False)
    if ret.failed and not fabric.contrib.console.confirm("Something went wrong doing the transfer, continue anyways? "):
        fabric.utils.abort("Aborting at user request")
#    logger.info("Doing simple copy of {0} to {1}".format(src, dest))
#    if not fabric.contrib.files.exists(dest):
#        ret = fabric.api.run("mkdir -p " + dest)
#        if ret.failed:
#            fabric.utils.abort(
#                "Failed to make directory {0}, likely due to permissions issue, aborting".format(dest))
#    elif fabric.contrib.console.confirm("{} already found, re-copy it?".format(src)):
#
#        ret = fabric.api.put(src, dest, use_sudo=False)
#        if ret.failed and not fabric.contrib.console.confirm("Something went wrong doing the transfer, continue anyways? "):
#            fabric.utils.abort("Aborting at user request")


def transfer(source, destination):
    ''' Puts source at destination on remote host, first queries if it exists and requets user guidence if so.  '''

    Rpath = destination + source.split('/')[-1:][0]
    logger.info("SRC: " + source)
    logger.info("DST: " + destination)
    logger.info("RPATH: " + Rpath)
    if fabric.contrib.files.exists(Rpath):
        if fabric.contrib.console.confirm(
                "{0} already exists on remote node, do you want to transfer again?".format(Rpath)):
            logger.info("\n> Transfering {} again".format(source))
            ret = fabric.api.put(source, destination, use_sudo=False)
            if ret.failed and not fabric.contrib.console.confirm("Something went wrong, continue anyways? "):
                fabric.utils.abort("Aborting at user request")
            else:
                logger.info("\n> Not transfering {} again, assuming current version is acceptable.".format(source))

    else:

        logger.info("\n> Does not {0} already exists on remote node".format(Rpath))
        ret = fabric.api.put(source, destination, use_sudo=False)
        if ret.failed and not fabric.contrib.console.confirm(
                "Something went wrong doing the transfer, continue anyways? "):
            fabric.utils.abort("Aborting at user request")


def remoteSetVar(var):
    ''' Appends 'export var' to ~/.bash_profile '''
    ret = fabric.api.run('echo "export {} ">> ~/.bash_profile'.format(var))
    if ret.failed and not fabric.contrib.console.confirm("Something went wrong, continue anyways? "):
        fabric.utils.abort("Aborting at user request")


def dependanciesCheck(ToQuery, dist):
    ''' Checks depednancies listed in ToInstall are available, uses fabric, returns missing dependancies '''
    missing = []
    err = 0
    if dist == 'ubuntu':
        for each in ToQuery:
            with hide('output', 'running', 'warnings'):
                ret = fabric.api.run('dpkg -s ' + str(each[0]))
            if ret.succeeded:
                each[1] = 0
            else:
                each[1] = 1

    elif dist == 'centos':
        for each in ToQuery:
            with hide('output', 'running', 'warnings'):
                each[1] = fabric.api.run('rpm -q ' + str(each[0]))
            if ret.succeeded:
                each[1] = 0
            else:
                each[1] = 1
    else:
        logger.error("\n> OS not recognised")

    for each in ToQuery:
        if each[1] == 1:
            missing.append(each[0])
            err = 1
    if err:
        return missing
    else:
        return 0


def configKafka(config, server):
    ''' Configure Kafka cluster machines '''
    ###### Carriesd out on all nodes as defined by deploy execute call ######
    # Set $K_HOME remotely
    remoteSetVar("K_HOME=" + config['HOME'])
    logger.info("\n> Set K_HOME ={}".format(config['HOME']))

    # Check required destination folders exists and if not create on remote nodes
    logger.info("\n> Checking for necessary Kafka directories {0}, {1}".format(config['zkdatadir'], config['logsdir']))
    existsCreate(config['zkdatadir'])
    existsCreate(config['logsdir'])

    # Add execution permissions to Kafka binaries
    chmodFolder(config['HOME'] + "/bin/", '-R a+x')
    logger.info('\n> Added execute permisions to {}/bin'.format(config['HOME']))

    ######	Carried out on local node and all indicated in calls made here #######
    # Setup and push Zookeeper config file
    zk = fabricsoodt.ZKConfigClass.ZK(config['zkdatadir'], config['NODES'], config['zkport'])
    zookeeperProperties = open("./fabricsoodt/templates/zookeeper.properties", 'w')
    zookeeperProperties.write(render.render(zk))
    zookeeperProperties.close()

    logger.info("\n> Transfering{0}, to {1}".format("./fabricsoodt/templates/zookeeper.properties",
                                                    config['HOME'] + "config/zookeerper.properties"))
    ret = fabric.api.put("./fabricsoodt/templates/zookeeper.properties",
                         config['HOME'] + "/config/zookeeper.properties", use_sudo=False)
    if ret.failed and not fabric.contrib.console.confirm("Something went wrong, continue anyways? "):
        logger.error("\n> Failed to put zookeeper.propertie")
        fabric.utils.abort("Aborting at user request")
    logger.info("\n> Pushing zookeeper.properties file to kafka server")


    # Create and push unique myid file
    fabric.api.local("echo {} > ./fabricsoodt/templates/myid".format(str(server)))
    logger.info("Creating myid with: {0}.  Pushing to: {1}".format(str(server), config))
    ret = fabric.api.put("./fabricsoodt/templates/myid", config['zkdatadir'], use_sudo=False)
    if ret.failed and not fabric.contrib.console.confirm("Something went wrong, continue anyways? "):
        logger.error("\n> Failed to put myid")
        fabric.utils.abort("Aborting at user request")
    logger.info("\n> Pushing myid file to kafka server")

    # Setup and distribute unique Kafka config files
    logger.info("\n> Creating and transferring Kafka configs for {}".format(str(server - 1)))
    ks = fabricsoodt.KSConfigClass.KS(server - 1, config['NODES'][server - 1], config['logsdir'], config['NODES'],
                                      config['zkport'], config['sport'])
    kafkaserverProperties = open("./fabricsoodt/templates/kafkaserver.properties", 'w')
    kafkaserverProperties.write(render.render(ks))
    kafkaserverProperties.close()
    ret = fabric.api.put("./fabricsoodt/templates/kafkaserver.properties", config['HOME'] + "/config/server.properties",
                         use_sudo=False)
    if ret.failed and not fabric.contrib.console.confirm("Something went wrong, continue anyways? "):
        logger.error("\n> Failed to put server.propertie")
        fabric.utils.abort("Aborting at user request")
    logger.info("\n> Pushing server.propertiesfile to kafka server")


def configMesos(config):
    ''' Configure Mesos cluster machines '''
    pass


def configure(app, config):
    ''' Select correct configuration mfunction '''

    if app == 'KAFKA':
        configKafka(config)
        logger.info("Configuringkafka")
    elif app == 'MESOS':
        configMesos(config)
    # elif app = 'SPARK':
    #	elif app = 'HADOOP':
    #	elif app = 'TACHYON':
    #	elif app = '':
    else:
        logging.error("\n> Applicaion for configuration not recognise")
        fabric.utils.abort("Aborting")

#################################???
# def chmodFolderToUser(user, folder):
#	run("chown " + user + " " + folder)
#	ret=run("chmod -R a+x "+folder)
#	if ret.failed and not fabric.contrib.console.confirm ("Something went wrong, Continue anyways? "):
#		abort("Aborting at user request")

####### Kafka deployment functions ######
#	""" Start Kafak broker cluster """
#
#
#	#Start zookeeper
#	with settings(warn_only=True):
#		ret=run('$ZK_HOME/bin/zkServer.sh start $ZK_HOME/conf/zoo_sample.cfg')
#	if ret.failed and not fabric.contrib.console.confirm ("Something went wrong, message: " +str(ret) + " Continue anyways? "):
#		abort("Aborting at user request")
#
#	#Start Kafka Cluster
#	with settings(warn_only=True):
#		run('$K_HOME/bin/kafka-server-start.sh $K_HOME/config/server.properties&')
#	if ret.failed and not fabric.contrib.console.confirm ("Something went wrong, message: " +str(ret) + " Continue anyways? "):
#		abort("Aborting at user request")
#
#	#Create cluster
#	#Create topics - how?:
