import logging
import os
import shutil

import dumpy

logger = logging.getLogger("dumper")

class FileSystemCopy(dumpy.base.PostProcessBase):
    """
    A post processor that copies the file to a specified path.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(FileSystemCopy, self).parse_config()
        self.dir = self._get_option_value(self.config, 'FileSystemCopy options', 'directory')
        override = self._get_option_value(self.config, 'database %s' % (self.db), 'FileSystemCopy directory')
        if override:
            self.dir = override

    def process(self, file):
        self.parse_config()

        base, ext = os.path.splitext(os.path.basename(file.name))
        if self.dir.endswith('/'):
            self.dir = self.dir[0:-1]
        new_file_name = '%s/%s%s' % (self.dir, base, ext)

        shutil.copy(file.name, new_file_name)
        logger.info('%s - %s - Copying %s to %s' % (self.db, self.__class__.__name__, file.name, new_file_name))

        return file

