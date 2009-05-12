import logging
import os

try:
    import boto
except ImportError:
    boto = None
else:
    from boto.s3.key import Key
    from boto.s3.connection import S3Connection

import dumpy

logger = logging.getLogger("dumper")

class S3Copy(dumpy.base.PostProcessBase):
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
        self.prefix = self._get_option_value(self.config, 'S3Copy options', 'prefix')
        # Make sure prefix ends with a single forward slash
        if not self.prefix.endswith('/'):
            self.prefix += '/'

    def process(self, file):
        if boto is None:
            raise Exception("You must have boto installed before using S3 support.")

        self.parse_config()

        conn = S3Connection(self.access_key, self.secret_key)
        bucket = conn.create_bucket(self.bucket)
        k = Key(bucket)
        if self.prefix:
            keyname = '%s%s' % (
                self.prefix,
                os.path.basename(file.name)
            )
        else:
            keyname = os.path.basename(file.name)
        k.key = keyname
        k.set_contents_from_file(file)

        logger.info('%s - %s - Copying to S3 with key name: %s' % (self.db, self.__class__.__name__, keyname))

        return file

