import datetime
import logging
import os
import shutil

import dumpy

logger = logging.getLogger("dumper")

class PrependDatabaseName(dumpy.base.PostProcessBase):
    """
    A post procesor that renames the file by prepending the database name to
    the existing file name.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(PrependDatabaseName, self).parse_config()

    def process(self, file):

        self.parse_config()

        dir = os.path.dirname(file.name)
        base, ext = os.path.splitext(os.path.basename(file.name))

        new_file_name = '%s/%s-%s%s' % (dir, self.db, base, ext)

        shutil.copy(file.name, new_file_name)
        logger.info('%s - %s - Copying %s to %s' % (self.db, self.__class__.__name__, file.name, new_file_name))
        new_file = open(new_file_name)
        file.close()

        return new_file

