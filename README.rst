=====
dumpy
=====

Dumpy is a Python database backup script that uses configuration files to
specify databases to backup and options.  Backup scripts are classes which
define a `backup` method.

Example configuration file
==========================

The following is an idea of what the configuration file, located at
`~/.dumpy.conf` might look like.  This is very likely to change::

	[databases]
	mysql = db1, db2
	postgresql = db3
	
	[mysql db1]
	user = db1
	pass = db1
	s3_copy = true
	
	[mysql db2]
	user = db2
	pass = db2
	
	[postgresql db3]
	user = db3
	pass = db3
	
	[mysqldump options]
	path = /opt/local/lib/mysql5/bin/mysqldump
	flags = -Q --opt --compact
	bzip = true
	
	[pgdump options]
	path = /opt/local/lib/postgresql83/bin/pg_dump
	
	[aws_s3]
	key = key
	secret = secret
	bucket = bucket

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
