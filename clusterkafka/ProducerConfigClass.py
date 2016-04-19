from pystache import template_spec


class PC(template_spec.TemplateSpec):
    """ Producer Config Pystache Class """

    def __init__(self,topic,partitionCount, zocP,msgsize,ID,inputFile):
        self.topic = topic
        self.partitionCount = partitionCount
        self.zocP = zocP
        self.msgsize=msgsize
        self.ID=ID
        self.inputFile=inputFile
        self.template_rel_directory = "templates/"
