import os
import ConfigParser
import tempfile

from boto.s3.key import Key
from boto.s3.connection import S3Connection

class MySQLDumpError(Exception):
    pass


class DumpyBase(object):
    """
    Overall base class for BackupBase and PostProcessBase.

    Provides a few utility methods to subclasses.
    """
    def _get_option_value(self, config, section, option, type=None):
        """
        Tries to get the section and option from config, returning None
        if not found.  Convert to type if given.
        """
        if not type or type not in ['boolean', 'float', 'int', 'string']:
            type = 'string'

        value = None
        try:
            if type == 'boolean':
                return config.getboolean(section, option)
            elif type == 'float':
                return config.getfloat(section, option)
            elif type == 'int':
                return config.getint(section, option)
            elif type == 'string':
                return config.get(section, option)
            else:
                return None
        except ConfigParser.NoSectionError:
            pass
        except ConfigParser.NoOptionError:
            pass

        return value

    def parse_config(self):
        """
        Subclasses parse their own config files since each will only need a
        subsection of the config.

        Example::

            super(SubClass, self).parse_config()
            options = {}
            try:
                for k, v in config.items('section'):
                    options[k] = v
            except ConfigParser.NoSectionError:
                pass # No section

        Or using the _get_option_value method::

            config = ConfigParser.SafeConfigParser()
            config.read(os.path.expanduser('~/.dumpy.cfg'))

            option1 = _get_option_value(config, 'section', 'option1')
            option2 = _get_option_value(config, 'section', 'option1', 'boolean')

        """
        self.config = ConfigParser.SafeConfigParser()
        self.config.read(os.path.expanduser('~/.dumpy.cfg'))

class BackupBase(DumpyBase):
    """
    Base class for database backups.
    """
    def backup(self):
        raise NotImplementedError

class PostProcessBase(DumpyBase):
    """
    Base class for post processing routines.
    """
    def process(self):
        raise NotImplementedError

class DatabaseBackup(BackupBase):
    """
    This classes loads the config's type and passes of backup to the type's
    class.  (e.g. type == 'mysql' calls MysqlBackup().backup().)
    """
    def __init__(self, database):
        self.database = database

    def parse_config(self):
        super(DatabaseBackup, self).parse_config()

        section = 'database %s' % (self.database)
        self.type = self._get_option_value(self.config, section, 'type')

    def backup(self):
        """
        A sort of proxy method to call the appropriate database type's backup
        method.
        """
        self.parse_config()
        if self.type == 'mysql':
            return MysqlBackup(self.database).backup()

class MysqlBackup(BackupBase):

    def __init__(self, database):
        self.database = database

    def parse_config(self):
        super(MysqlBackup, self).parse_config()

        section = 'database %s' % (self.database)
        self.user = self._get_option_value(self.config, section, 'user')
        self.password = self._get_option_value(self.config, section, 'password')
        self.host = self._get_option_value(self.config, section, 'host')
        self.port = self._get_option_value(self.config, section, 'port', 'int')

        self.binary = self._get_option_value(self.config, 'mysqldump options', 'path')
        self.flags = self._get_option_value(self.config, 'mysqldump options', 'flags')

    def get_flags(self):
        flags = '%s' % (self.flags)
        if self.user:
            flags += ' -u %s' % (self.user)
        if self.password:
            flags += ' -p%s' % (self.password)
        if self.host:
            flags += ' -h %s' % (self.host)
        if self.port:
            flags += ' -P %d' % (self.port)
        return flags

    def backup(self):
        self.parse_config()
        tmp_file = tempfile.NamedTemporaryFile()
#        try:
        cmd = '%(binary)s %(flags)s %(database)s > %(file)s' % ({
            'binary': self.binary,
            'flags': self.get_flags(),
            'database': self.database,
            'file': tmp_file.name,
        })
        print cmd #FIXME
        os.system(cmd)

        return tmp_file

class PostProcess(PostProcessBase):
    """
    This classes loads the specified database `postprocessing` config option
    and passes off handling to each post processor.
    """
    def __init__(self, database):
        self.database = database

    def parse_config(self):
        super(PostProcess, self).parse_config()

        self.processors = self._get_option_value(self.config, 'database %s' % (self.database), 'postprocessing')

    def process(self, file):
        self.parse_config()
        processors = [p.strip() for p in self.processors.split(',')]

        for processor in processors:
            print processor

class Bzip(PostProcessBase):
    """
    A post processor that bzips the given file and returns it.
    """
    def __init__(self, file):
        self.file = file

    def parse_config(self):
        super(Bzip, self).parse_config()

        self.path = self._get_option_value(self.config, 'Bzip options', 'path')

    def process(self):
        cmd = "%(path)s %(file)s"
        file = open('%s.bz2' % (self.file.name))
        self.file.close()
        return file

class SystemFileCopy(PostProcessBase):
    """
    A post processor that copies the file to a specified path.
    """
    pass

class S3Copy(PostProcessBase):
    """
    A post processor that copies the given file to S3.
    """

    def s3_connect(self):
        try:
            conn = S3Connection(self.access_key, self.secret_key)
            self.is_connected = True
            return conn
        except:
            self.is_connected = False

    def open_bucket(self, bucketname):
        if not self.is_connected:
            conn = self.s3_connect()
        self.bucket = conn.create_bucket(bucketname)
        return
   
    def open_key(self, keyname):
        if not self.is_connected:
            conn = self.s3_connect()
        k = Key(self.bucket)
        k.key = keyname
        self.keyname = keyname
        return k

    def backup(self):
        self.open_bucket(self.bucket)
        k = self.open_key(self.filename)
        k.set_contents_from_filename(self.filename)
        return

if __name__ == '__main__':

    # Process options:
    # dumpy --database [database name]

    # Call DatabaseBackup first
    file = DatabaseBackup('test_db').backup()

    # Then call post processors, in the given order
    PostProcess('test_db').process(file)

