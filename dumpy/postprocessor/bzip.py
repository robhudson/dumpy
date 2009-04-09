import logging
import os

import dumpy

logger = logging.getLogger("dumper")

class Bzip(dumpy.base.PostProcessBase):
    """
    A post processor that bzips the given file and returns it.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(Bzip, self).parse_config()
        self.path = self._get_option_value(self.config, 'Bzip options', 'path')

    def process(self, file):

        self.parse_config()

        cmd = "%(path)s -f '%(file)s'" % ({'path': self.path, 'file': file.name})
        logger.info('%s - %s - Command: %s' % (self.db, self.__class__.__name__, cmd))
        os.system(cmd)
        new_file = open('%s.bz2' % (file.name))
        file.close()
        return new_file

