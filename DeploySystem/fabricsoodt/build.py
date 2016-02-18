""" Module containing functions for the extraction, building, and pushing of applications """
#External imports
import platform
import logging 

#Internal imports
import fabricsoodt.setup
import fabric.api
fabric.api.env.warn_only=True


logger=logging.getLogger("fabricsoodt.build")


def extract (fullname, destination):
	""" Extracts an application to a destination unsing the appropriate tar or unzip based on its file extension """
	if fullname.endswith('.tgz'):	#tar gzip
		#os.system('tar -xzvf %s -C %s' % (location+application,location))
		fabric.api.local('tar --skip-old-files -xzf ' + fullname +' -C ' + destination)
		dirname = fullname.split('/')[-1:][0][:-4]
		logger.info("\n> Untared {0} to {1}, returning resulting directory name: {2}".format(fullname, destination,dirname))
		return dirname

	if fullname.endswith('.tar.gz'):	#tar gzip
		#os.system('tar -xzvf %s -C %s' % (location+application,location))
		fabric.api.local('tar --skip-old-files -xzf ' + fullname +' -C ' + destination)
		dirname = fullname.split('/')[-1:][0][:-7]
		logger.info("\n> Untared {0} to {1}, returning resulting directory name: {2}".format(fullname, destination,dirname))
		return dirname

	elif fullname.endswith('.zip'):	# zip
		#os.system('unzip -x %s -d %s' % (location+application,location))
		fabric.api.local(' unzip -xn ' + fullname + ' -d ' + destination)
		dirname = fullname.split('/')[-1:][0][:-4]
		logger.info("\n> Unzipped {0} to {1}, returning resulting directory name: {2}".format(fullname, destination,dirname))
		return dirname

def localDependencies (dist, ToInstall):
	""" For local operation only: Updates the system, checks if dependancies in list 'ToInstall' are installed in a distro appropriate manner"""

	if dist == 'ubuntu':
		ret=fabric.api.local('sudo apt-get update')
		if ret.failed and not fabric.contrib.console.confirm("Update failed, do you want to continue?"):
			logger.info("\n> Aborting due to failure to update")
			fabric.utils.abort("Aborting")
		
		for each in ToInstall:
			each[1] = fabric.api.local('sudo apt-get install -y ' + each[0]).return_code
		
	elif dist == 'centos':
		ret=fabric.api.local('sudo yum update')
		if ret.failed and not fabric.contrib.console.confirm("Update failed, do you want to continue?"):
			logger.info("\n> Aborting due to failure to update")
			fabric.utils.abort("Aborting")
	
		for each in ToInstall:
			each[1] = fabric.api.local('sudo yum install -y ' + each[0]).return_code

	return ToInstall
#	elif dist == 'redhat': 
#	elif dist == 'fedora': 
#	elif dist == 'debian':

def buildMesos (fullname, sudo, test):
	""" Build the mesos package, return 1 on success, 0 on failure """
	fabric.api.env=True	
	logger.info("\n> Building mesos package")

	#Check if platform is 64b:
	logger.info("\n> Checking the OS and platform are suitable")
	if platform.machine() != 'x86_64' and platform.machine() != 'amd64' and not platform.linux_distribution():
		logger.error("\n> It appears this system does not meet the requirments for Mesos")
		if not confirm("Our checking indicates this machine is either not 64b or not running a linux distribution, both of which are required for Mesos, if you believe we are mistaken and wish to continue anyways please confirm with 'y' otherwise we will abort here."):
			fabric.utils.abort("Aborting, machine is not suitable for building Mesos")

	#Get distribution:
	dist = fabricsoodt.setup.getos()
	logger.info("\n> Checking distro, found: {0}".format(dist))
	
	#Install dependencies if sudo enabled otherwise assume they are present:
	if fabric.api.local('mvn --version').return_code:
		logger.error("\n> Maven not found, aborting, please install")	
		fabric.utils.abort("Aborted")
	
	#Ubuntu
	if dist == 'ubuntu':
		ToInstall = [['build-essential',1],['openjdk-7-jdk',1],['python-dev',1],['python-boto',1],['libcurl4-nss-dev',1],['maven',1],['libsasl2-dev',1],['libapr1-dev',1],['libsvn-dev',1]]
		if sudo:
			logger.info("\n> Using sudo to install dependencies")
			ToInstall=localDependencies(dist, ToInstall)
		else:
			logger.info("\n> Sudo not enabled, checking for installed dependencies")
			for each in ToInstall:
					each[1] = fabric.api.local("dpkg -s "+each[0]).return_code


	#If Centos
	elif dist == 'centos':

		ToInstall = [['python-devel',1]	,['java-1.7.0-openjdk-devel',1]	,['zlib-devel',1],['libcurl-devel',1],['openssl-devel',1],['cyrus-sasl-devel',1],['cyrus-sasl-md5',1],['apr-devel',1],['subversion-devel',1],['apr-util-devel',1],['python-boto',1]]
		
		if sudo:

			#Add repo containing subvision-devel 1.8
			fabric.api.local("sudo echo \"[WanddiscoSVN]\n name=Wandisco SVN Repo \nbaseurl=http://opensource.wandisco.com/centos/6/svn-1.8/RPMS/$basearch/ \nenabled=1 \ngpgcheck=0\" > /etc/yum.repos.d/wandisco-svn.repo")

			#Manually install "Development tools"
			ret = fabric.api.local('sudo yum group install -y "Development Tools"')
			if ret.failed: 
				logger.error("\n> Aborting due to failure to group install \"Development Tools\"")
				fabric.utils.abort("Aborted")
			
			logger.info("\n> Using sudo to install dependancies")
			ToInstall=localDependencies(dist, ToInstall)
	
			for each in ToInstall:
					each[1] = fabic.api.local("dpkg -s "+each[0]).return_code

		else:
			logger.info("\n> Sudo not enabled, checking for installed dependencies")
			for each in ToInstall:
				each[1] = fabric.api.local('rpm -q ' + each[0]).return_code

#	elif dist == 'redhat':
#	elif dist == 'fedora':
#	elif dist == 'debian':

	else:
		logger.error("\n> OS {0}not recognised, cannot ensure dependendcies are installed, aborting".format(dist))
		fabric.utils.abort("Aborting")

#Print dependency installation results
	for each in ToInstall:
		if each[1] == 0:
			logger.info("\n> {} is installed and available.".format(each[0]))
			dependency_exit = False
		else:
			logger.error("\n> Failed to install: "+each[0])
			dependency_exit = True

	if dependency_exit:
		logger.error("\n> The above dependencies were not available or could not be installed, please install and then re-run")
		fabric.utils.abort("Aborting due to the above dependancies being missing:")

#Build Mesos
	buildfolder = fullname[:-7]+"/build"
	fabric.api.local('mkdir ' + buildfolder)
	logger.info("\n> Created build folder {0}".format(buildfolder))

	try:
		#subprocess.Popen("../configure",cwd=buildfolder)
		with fabric.api.lcd(buildfolder):
			ret = fabric.api.local("../configure")
			if ret.failed:
				logger.error("\n> Failed to run configure in Mesos build folder")
				fabric.api.abort("Aborting, failed to execute ../configue")
		logger.info("\n> Completed ./configure")

	except KeyboardInterrupt:
		fabric.api.abort("User aborted")

	try:	
		#subprocess.Popen("make",cwd=buildfolder).communicate()
		with fabric.api.lcd(buildfolder):
			fabric.api.local("make")
		logger.info("\n> Completed make")

	except KeyboardInterrupt:
		fabric.api.abort("User aborted")
		
	if test:
		try:
			#subprocess.Popen(["make", "check"],cwd=buildfolder).communicate()
			with fabric.api.lcd(buildfolder):
				fabric.api.local("make  check")
			logger.info("\n> Complete make check ######")

		except KeyboardInterrupt:
			fabric.api.abort("User aborted")

	return 0 

def build (app, fullname, sudo, test):
	""" Generic build function to call application specific build.
		Returns:
		An array of [x, message], where x is 1 on success and 0 on error
	"""
	#This hierarchical build call exists such that further application specific build functions can be written later 
	if app == 'MESOS':
		ret = buildMesos(fullname, sudo, test)
		msg = "Successfully built Mesos"

#	elif app == 'OODT':
#		ret = build_oodt(fullname, sudo, test)
#		msg = "Successfully build OODT"
#
#	elif app ==
#
	else:
		msg = "Don't know how to build {0}".format(app)

	return [ret,msg]
