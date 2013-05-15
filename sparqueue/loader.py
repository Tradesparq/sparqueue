import importlib
import os.path
import sys

import logging
logger = logging.getLogger(__file__)

class JobLoader():
    def __init__(self):
        self.classname_to_class = {}
        self.classname_to_object = {}
        self.classname_to_timestamp = {}
        self.classname_to_modulepath = {}
        self.classname_to_module = {}

    def getClass(self, classname):
        if classname not in self.classname_to_class:
            self.classname_to_class[classname] = self._getClass(classname)
        return self.classname_to_class[classname]

    def getInstance(self, classname, config, parameters=None):
        if classname in self.classname_to_object:
            mtime = os.path.getmtime(self.classname_to_modulepath[classname])
            if mtime > self.classname_to_timestamp[classname]:
                del self.classname_to_object[classname]
                m = self.classname_to_module[classname]
                del self.classname_to_module[classname]
                logger.info('reloading %s' % classname)
                self.classname_to_module[classname] = reload(m)
                self.classname_to_timestamp[classname] = mtime

        if classname not in self.classname_to_object:
            self.classname_to_object[classname] = self.getClass(classname).instantiate(config, parameters)

        return self.classname_to_object[classname]

    def _getClass(self, classname):
        components = classname.split('.')
        module_name = '.'.join(components[0:-1])
        classname_only = components[-1]
        m = importlib.import_module(module_name)
        module_filename = os.path.abspath(sys.modules[module_name].__file__)
        self.classname_to_modulepath[classname] = module_filename
        self.classname_to_timestamp[classname] = os.path.getmtime(module_filename)
        self.classname_to_module[classname] = m

        return getattr(m, classname_only)