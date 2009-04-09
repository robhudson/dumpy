import logging
import os
import tempfile

import dumpy

logger = logging.getLogger("dumper")

class PostgresqlBackup(dumpy.base.BackupBase):

    def parse_config(self):
        super(PostgresqlBackup, self).parse_config()

        section = 'database %s' % self.db
        self.name = self._get_option_value(self.config, section, 'name')
        self.user = self._get_option_value(self.config, section, 'user')
        self.host = self._get_option_value(self.config, section, 'host')
        self.port = self._get_option_value(self.config, section, 'port', 'int')

        self.binary = self._get_option_value(self.config, 'pg_dump options', 'path')
        self.flags = self._get_option_value(self.config, 'pg_dump options', 'flags')

    def get_flags(self):
        # @@@ ugh. i don't like this.
        if self.flags is None:
            flags = ''
        else:
            flags = '%s' % self.flags
        if self.user:
            flags += ' -U %s' % self.user
        if self.host:
            flags += ' -h %s' % self.host
        if self.port:
            flags += ' -p %d' % self.port
        return flags

    def backup(self):
        self.parse_config()
        tmp_file = tempfile.NamedTemporaryFile()
        cmd = '%(binary)s %(flags)s %(database)s > %(file)s' % {
            'binary': self.binary,
            'flags': self.get_flags(),
            'database': self.name,
            'file': tmp_file.name,
        }
        logger.info('%s - Command: %s' % (self.db, cmd))
        os.system(cmd)
        return tmp_file

