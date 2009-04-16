import logging
import os
import shutil

import dumpy

logger = logging.getLogger("dumper")

class RotateFiles(dumpy.base.PostProcessBase):
    """
    A post processor that rotates files in a directory to keep an optional
    number on file before deleting them.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(RotateFiles, self).parse_config()
        self.dir = self._get_option_value(self.config, 'RotateFiles options', 'directory')
        self.num = self._get_option_value(self.config, 'RotateFiles options', 'number', 'int')
        if not self.num:
            self.num = 10

    def process(self, file):
        self.parse_config()

        base, ext = os.path.splitext(os.path.basename(file.name))
        if self.dir.endswith('/'):
            self.dir = self.dir[0:-1]
        basename = '%s/%s%s' % (self.dir, base, ext)

        for i in range(self.num, 0, -1):
            if self.num == i: # We remove last one
                if os.path.exists("%s.%d" % (basename, i)):
                    os.remove("%s.%d" % (basename, i))
                    logger.info('%s - %s - Remove last file %s.%d' % (
                        self.db, self.__class__.__name__, basename, i))
            if i > 1: # Copy n-1 to n (e.g. name.1 to name.2)
                if os.path.exists("%s.%d" % (basename, i-1)):
                    os.rename("%s.%d" % (basename, i-1), "%s.%d" % (basename, i))
                    logger.info('%s - %s - Copy file %s.%d to %s.%d' % (
                        self.db, self.__class__.__name__,
                        basename, i-1, basename, i))
            elif i == 1: # name.1 is a copy of the original
                shutil.copy(file.name, "%s.%d" % (basename, i))
                logger.info('%s - %s - Copy file %s to %s.%d' % (
                    self.db, self.__class__.__name__,
                    file.name, basename, i))

        return file

