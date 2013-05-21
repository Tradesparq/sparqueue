import importlib
import os.path
import sys

import logging

logger = logging.getLogger(__file__)


class JobLoader():
    def __init__(self):
        self.clazz_to_class = {}
        self.clazz_to_object = {}
        self.clazz_to_timestamp = {}
        self.clazz_to_modulepath = {}
        self.clazz_to_module = {}

    def getClass(self, clazz):
        if clazz not in self.clazz_to_class:
            self.clazz_to_class[clazz] = self._getClass(clazz)
        return self.clazz_to_class[clazz]

    def getInstance(self, clazz, config, parameters=None):
        if clazz in self.clazz_to_object:
            mtime = os.path.getmtime(self.clazz_to_modulepath[clazz])
            if mtime > self.clazz_to_timestamp[clazz]:
                del self.clazz_to_object[clazz]
                m = self.clazz_to_module[clazz]
                del self.clazz_to_module[clazz]
                logger.info('reloading %s' % clazz)
                self.clazz_to_module[clazz] = reload(m)
                self.clazz_to_timestamp[clazz] = mtime

        if clazz not in self.clazz_to_object:
            clazz = self.getClass(clazz)
            self.clazz_to_object[clazz] = clazz.instantiate(
                config, parameters)

        return self.clazz_to_object[clazz]

    def _getClass(self, clazz):
        components = clazz.split('.')
        module_name = '.'.join(components[0:-1])
        clazz_only = components[-1]
        m = importlib.import_module(module_name)
        module_filename = os.path.abspath(sys.modules[module_name].__file__)
        self.clazz_to_modulepath[clazz] = module_filename
        self.clazz_to_timestamp[clazz] = os.path.getmtime(module_filename)
        self.clazz_to_module[clazz] = m

        return getattr(m, clazz_only)
