import datetime
import logging
import os
import shutil

import dumpy

logger = logging.getLogger("dumper")

class TimestampRename(dumpy.base.PostProcessBase):
    """
    A post procesor that renames the file using timestamp format.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(TimestampRename, self).parse_config()
        self.format = self._get_option_value(self.config, 'TimestampRename options', 'format')
        self.insert_db_name = self._get_option_value(self.config, 'database %s' % (self.db), 'insert_db_name', 'boolean')

    def process(self, file):

        self.parse_config()

        dir = os.path.dirname(file.name)
        base, ext = os.path.splitext(os.path.basename(file.name))
        # @@@ Clint: Put DB name into renamed name, I dunno how i like this in here
        file_name_format = '%s/%s%s'
        if self.insert_db_name:
          file_name_format = '%s/'+self.db+'-%s%s'
        new_file_name = file_name_format % (dir, datetime.datetime.now().strftime(self.format), ext)

        shutil.copy(file.name, new_file_name)
        logger.info('%s - %s - Copying %s to %s' % (self.db, self.__class__.__name__, file.name, new_file_name))
        new_file = open(new_file_name)
        file.close()
        return new_file

