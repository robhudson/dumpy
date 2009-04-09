#!/usr/bin/env python
import ConfigParser
import logging
import optparse
import os
import sys

from dumpy import base

parser = optparse.OptionParser()
parser.add_option("-D", "--database",
                  dest="database",
                  default='db1',
                  help="Dump only the specified database with matching config name")
parser.add_option("-v", "--verbose",
                  action="store_true",
                  dest="verbose",
                  default=False,
                  help="Display logging output")
parser.add_option("-a", "--all-databases",
                  action="store_true",
                  dest="all",
                  default=False,
                  help="Dump all databases in the configuration file")

(options, args) = parser.parse_args()

logger = logging.getLogger("dumper")
logger.setLevel(logging.ERROR)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

if options.verbose:
    logger.setLevel(logging.DEBUG)

dbs_to_dump = []

if options.all:
    config = ConfigParser.SafeConfigParser()
    config.read(os.path.expanduser('~/.dumpy.cfg'))
    sections = config.sections()
    for db in sections:
        if db.startswith('database '):
            dbname = db.replace('database ', '')
            dbs_to_dump.append(dbname)
else:
    dbs_to_dump.append(options.database)

for db in dbs_to_dump:
    file = base.DatabaseBackup(db).backup()
    # Then call post processors, in the given order
    file = base.PostProcess(db).process(file)
