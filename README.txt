Python Fabric based Streaming OODT install.

---------
OPERATION
---------

- Ensure passkey access to deploying nodes
- Setup config file.	
	-- A template of which is found in ./config_template.ini  All parameters found here must be set, comment out sections for aplication which you do not wish to have installed.
	-- Where greater configuration of applications is desired than is made available via this config file, templates contains application-specific config file templates.  These may be edited.  Any changes made here will propigate to all nodes receiving this file.
- Ensure 5GB RAM available for Tachyon
- Ensure the listed dependancise are installed on the machine from which this install application will be run.  
- Where build commands are to be issued the node on which this script is called must be of the same architecture as deploy nodes
- To run:
		Install: $ python deploy install configs/config.ini
		Clean: $ python deploy clean configs/config.ini
- Log files are found in ./logs currently.  Edit the logging setup in deploy for changes in fabricsoodt reporting, all loggers are named as subsidary to fabricsoodt'

------------
DEPENDANCIES
------------
Python 3
python-dev
python-pip
pip modules*:
	pystache
	configparser
    yarn
scala
maven 2.x+
openjdk-7-jdk

*Use < pip install module-name --user > to install in user space (no sudo required)

$ pip install -r requirements.txt
$ cd DeploymentSystems/Yarn
$ python setup.py install

Hint: for colour logs install coloredlogs 
$ pip install coloredlogs
And run with -cl or --colouredLogs flag

Some optional applications have their own dependencies.  Given that this install system is intended to not require sudo the user is expected to ensure the following are installed. If sudo access is available set this in the config script and attempts to auto install will be made

Mesos:
 - build-essentials
 - python-dev
 - python-boto
 - libcurl4-nss-dev
 - maven	(*Only required for Mesos 0.18.1 or newer*)
 - libsvn-dev	(*Only required for Mesos 0.21.0 or newer)
 - libsasl2-dev	(*Only required for Mesos 0.14.0 or newer*)
 - libapr1-dev 	(*Only required for Mesos 0.21.0 or newer*)


----------------------------
Centos specific instructions
----------------------------
 - To install pip with sudo:
	 - Add EPEL repository
		sudo pm -iUvh http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm
		sudo yum -y update
		sudo yum -y install python-pip
 - Or to install pip locally without sudo:
	 - curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
	 - python get-pip.py --user
