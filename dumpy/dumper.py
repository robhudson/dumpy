import os
import tempfile

from boto.s3.key import Key
from boto.s3.connection import S3Connection

class MySQLDumpError(Exception):
    pass


class Backup(object):
    def backup(self):
        raise NotImplementedError

class MysqlBackup(Backup):
    def __init__(self, database, username, password, host=None, binary=None, flags=None, bzip=False):
        self.database = database
        self.username = username
        self.password = password
        self.host = host and host or 'localhost'
        if binary:
            self.binary = binary
        else:
            pass # Search for mysqldump?
        self.flags = flags
        self.bzip = bzip

    def backup(self):
        tmp_file = tempfile.NamedTemporaryFile()
#        try:
        cmd = '%(binary)s %(flags)s %(database)s %(bzip)s > %(file)s' % ({
            'binary': self.binary,
            'flags': self.get_flags(),
            'database': self.database,
            'file': tmp_file.name,
            'bzip': self.bzip and '|bzip2' or '',
        })
        print cmd
        os.system(cmd)
#        except:
#            print cmd
#            raise MySQLDumpError, "Mysqldump command failed!"

        # Return temp file?
        # Then we can copy it somewhere? or push it up to S3?
        # Close it? Can OS read it if it's already opened?
        # Maybe just return name to be opened by other methods?
        return tmp_file

    def get_flags(self):
        flags = '%s' % (self.flags)
        flags += ' -u %s' % (self.username)
        flags += ' -p%s' % (self.password)
        flags += ' -h %s' % (self.host)
        return flags

class S3Backup(Backup):
    def __init__(self, access_key, secret_key, bucket, filename):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.filename = filename
        self.is_connected = False

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
    # If called as a script directly, parse config and run...
    import ConfigParser

    config = ConfigParser.SafeConfigParser()
    config.read(os.path.expanduser('~/.dumpy.conf'))

    aws_s3 = {}
    try:
        for k, v in config.items('aws_s3'):
            aws_s3[k] = v
    except ConfigParser.NoSectionError:
        pass # No AWS S3 options set
    
    databases = {}
    try:
        for k, v in config.items('databases'):
            databases[k] = [db.strip() for db in v.split(',')]
    except ConfigParser.NoSectionError:
        pass # No databases
    
    for db_type, db_list in databases.iteritems():
        
        if db_type == 'mysql':
            mysql_options = {}
            try:
                for k, v in config.items('mysqldump options'):
                    mysql_options[k] = v
                # Convert bzip to boolean
                mysql_options['bzip'] = config.getboolean('mysqldump options', 'bzip')
            except ConfigParser.NoSectionError:
                pass

        for db in db_list:
            
            print "Performing %s dump for: %s" % (db_type, db)
    
            section = 'mysql %s' % (db)
            host = None
            s3_copy = False
            try:
                username = config.get(section, 'user')
                password = config.get(section, 'pass')
                if config.has_option(section, 'host'):
                    host = config.get(section, 'host')
                if config.has_option(section, 's3_copy'):
                    s3_copy = config.getboolean(section, 's3_copy')
            except ConfigParser.NoOptionError:
                pass
        
            backup = MysqlBackup(db, username, password, host,
                binary=mysql_options['path'],
                flags=mysql_options['flags'],
                bzip=mysql_options['bzip']
            )
            tmp_file = backup.backup()
            if s3_copy and aws_s3:
                print "Copying file '%s' to S3" % (tmp_file.name)
                #s3backup = S3Backup(aws_s3['key'], aws_s3['secret'], aws_s3['bucket'], tmp_file.name)
                #s3backup.backup()

