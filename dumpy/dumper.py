import os
import ConfigParser
import datetime
import shutil
import tempfile

try:
    import boto
except ImportError:
    boto = None
else:
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

    Any subclass of BackupBase needs to implement the backup() method and
    return a file-like object.
    """
    def __init__(self, db):
        self.db = db

    def backup(self):
        raise NotImplementedError

class PostProcessBase(DumpyBase):
    """
    Base class for post processing routines.

    Any subclass of PostProcessBase needs to implement the process(file)
    method, taking in a file-like object and returning a file-like object.  The
    process(file) method should return the file object passed in if unchanged.
    """
    def process(self, file):
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
        elif self.type == 'postgresql':
            return PostgresqlBackup(self.database).backup()

class MysqlBackup(BackupBase):

    def parse_config(self):
        super(MysqlBackup, self).parse_config()

        section = 'database %s' % (self.db)
        self.name = self._get_option_value(self.config, section, 'name')
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
            'database': self.name,
            'file': tmp_file.name,
        })
        print cmd #FIXME
        os.system(cmd)

        return tmp_file

class PostgresqlBackup(BackupBase):

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
        print cmd # FIXME
        os.system(cmd)
        return tmp_file

class PostProcess(PostProcessBase):
    """
    This classes loads the specified database `postprocessing` config option
    and passes off handling to each post processor.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(PostProcess, self).parse_config()

        self.processors = self._get_option_value(self.config, 'database %s' % (self.db,), 'postprocessing')

    def process(self, file):
        self.parse_config()

        if self.processors:
            processors = [p.strip() for p in self.processors.split(',')]

            for processor in processors:
                print processor #FIXME
                file = globals()[processor](self.db).process(file)

class Bzip(PostProcessBase):
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
        print cmd #FIXME
        os.system(cmd)
        new_file = open('%s.bz2' % (file.name))
        file.close()
        return new_file

class TimestampRename(PostProcessBase):
    """
    A post procesor that renames the file using timestamp format.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(TimestampRename, self).parse_config()
        self.format = self._get_option_value(self.config, 'TimestampRename options', 'format')

    def process(self, file):

        self.parse_config()

        dir = os.path.dirname(file.name)
        base, ext = os.path.splitext(os.path.basename(file.name))
        new_file_name = '%s/%s%s' % (dir, datetime.datetime.now().strftime(self.format), ext)

        shutil.copy(file.name, new_file_name)
        new_file = open(new_file_name)
        file.close()
        return new_file

class SystemFileCopy(PostProcessBase):
    """
    A post processor that copies the file to a specified path.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(SystemFileCopy, self).parse_config()
        self.dir = self._get_option_value(self.config, 'SystemFileCopy options', 'directory')
        override = self._get_option_value(self.config, 'database %s' % (self.db), 'SystemFileCopy directory')
        if override:
            self.dir = override

    def process(self, file):
        self.parse_config()

        dir = os.path.dirname(file.name)
        base, ext = os.path.splitext(os.path.basename(file.name))
        if self.dir.endswith('/'):
            self.dir = self.dir[0:-1]
        new_file_name = '%s/%s%s' % (self.dir, base, ext)

        shutil.copy(file.name, new_file_name)
        new_file = open(new_file_name)
        file.close()
        return new_file

class S3Copy(PostProcessBase):
    """
    A post processor that copies the given file to S3.
    """
    def __init__(self, db):
        self.db = db

    def parse_config(self):
        super(S3Copy, self).parse_config()
        self.access_key = self._get_option_value(self.config, 'S3Copy options', 'access_key')
        self.secret_key = self._get_option_value(self.config, 'S3Copy options', 'secret_key')
        self.bucket = self._get_option_value(self.config, 'S3Copy options', 'bucket')

    def process(self, file):
        if boto is None:
            raise Exception("You must have boto installed before using S3 support.")
        
        self.parse_config()

        conn = S3Connection(self.access_key, self.secret_key)
        bucket = conn.create_bucket(self.bucket)
        k = Key(bucket)
        k.key = file.name
        k.set_contents_from_file(file)

        return file

if __name__ == '__main__':

    # Process options:
    # dumpy --database [database name]

    # Call DatabaseBackup first
    file = DatabaseBackup('db1').backup()

    # Then call post processors, in the given order
    PostProcess('db1').process(file)

