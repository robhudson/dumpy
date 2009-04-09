=====
dumpy
=====

Dumpy is a Python database backup script that uses configuration files to
specify databases to backup and options.  Backup scripts are classes which
define a `backup` method.

Each `BackupBase` subclass returns a `NamedTemporaryFile` object.  It's up to
any post processors to use this object in any way (e.g. copy it to another
location on the file system).

Post processors can be chained and all take the form::

	MyPostProcessor().process(file)

If the post process doesn't alter the file passed in it should return it
unchanged.

Example configuration file
==========================

The following is an idea of what the configuration file, located at
`~/.dumpy.conf` might look like.  This is very likely to change::

	[database db1]
	type = mysql
	name = dbname1
	user = db1
	password = db1
	postprocessing = TimestampRename, Bzip, SystemFileCopy, S3Copy
	
	[database db2]
	type = postgresql
	name = dbname2
	user = db2
	password = db2
	postprocessing = TimestampRename, Bzip, SystemFileCopy
	
	[mysqldump options]
	path = /opt/local/lib/mysql5/bin/mysqldump
	flags = -Q --opt --compact
	
	[pg_dump options]
	path = /opt/local/lib/postgresql83/bin/pg_dump
	
	[TimestampRename options]
	format = %%Y%%m%%d
	
	[Bzip options]
	path = /usr/bin/bzip2
	
	[S3Copy options]
	access_key = access_key
	secret_key = secret_key
	bucket = bucket
	prefix = path/to/directory


Status
======

Very alpha.  Looking for other coders to help and flesh out ideas.

Motivation
==========

I've written my last database dump and backup script that I want to.  My hope
is that this will be a general and feature rich backup script that's easily
extendable and will work across multiple databases and backup schemes.

Future plans
============

* Support some file based backups with auto rotation.
* Finish S3 Backup and flesh out S3 options.
* Lots more to think of.
