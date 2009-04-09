import ConfigParser
import os

from dumpy.importlib import import_module

class ProcessorException(Exception):
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
            from dumpy.database import mysql
            return mysql.MysqlBackup(self.database).backup()
        elif self.type == 'postgresql':
            from dumpy.database import postgresql
            return postgresql.PostgresqlBackup(self.database).backup()

class PostProcess(PostProcessBase):
    """
    This classes loads the specified database `postprocessing` config option
    and passes off handling to each post processor.
    """
    def __init__(self, db):
        self.db = db
        self.builtin_processors = {
            'Bzip': 'dumpy.postprocessor.bzip.Bzip',
            'TimestampRename': 'dumpy.postprocessor.timestamp.TimestampRename',
            'FileSystemCopy': 'dumpy.postprocessor.fscopy.FileSystemCopy',
            'S3Copy': 'dumpy.postprocessor.s3copy.S3Copy',
        }

    def parse_config(self):
        super(PostProcess, self).parse_config()

        self.processors = self._get_option_value(self.config, 'database %s' % (self.db,), 'postprocessing')

    def process(self, file):
        self.parse_config()

        if self.processors:
            processors = [p.strip() for p in self.processors.split(',')]

            for processor_path in processors:
                if processor_path in self.builtin_processors.keys():
                    processor_path = self.builtin_processors.get(processor_path)

                try:
                    dot = processor_path.rindex('.')
                except ValueError:
                    raise ProcessorException, '%s isn\'t a processor module' % processor_path
                pp_module, pp_classname = processor_path[:dot], processor_path[dot+1:]
                try:
                    mod = import_module(pp_module)
                except ImportError, e:
                    raise ProcessorException, 'Error importing processor %s: "%s"' % (pp_module, e)
                try:
                    pp_class = getattr(mod, pp_classname)
                except AttributeError:
                    raise ProcessorException, 'Processor module "%s" does not define a "%s" class' % (pp_module, pp_classname)

                processor = pp_class(self.db)

                file = processor.process(file)

