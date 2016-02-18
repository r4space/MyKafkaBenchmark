from pystache import template_spec


class CC(template_spec.TemplateSpec):
    """ Consumer Config Pystache Class """

    def __init__(self, zocC, groupID, topic, partitionCount, ID):
        self.zocC = zocC
        self.groupID = groupID
        self.topic = topic
        self.partitionCount = partitionCount
        self.ID = ID
        self.template_rel_directory = "templates/"