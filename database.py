#!/usr/bin/env python

#############################################################################
#	database.py																#
#																			#
#	Author:		Adam Baker (ambaker@usgs.gov)								#
#	Date:		2016-05-17													#
#	Version:	0.5.4														#
#																			#
#	Purpose:	Allows for quicker implementation of a database				#
#############################################################################

import psycopg2

class Database(object):
	def __init__(self, dbname = None, user = None, host = None, password = None):
		'Initializes the connection to the database'
		self._dbname = dbname
		self._user = user
		self._host = host
		self._password = password
		self.open_connection(self._dbname, self._user, self._host, self._password)
		self.populate_table_names_and_fields()
	def open_connection(self, dbname = None, user = None, host = None, password = None):
		'Opens the connection to the database'
		if dbname: self._dbname = dbname
		if user: self._user = user
		if host: self._host = host
		if password: self._password = password
		self.conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (self._dbname, self._user, self._host, self._password))
	def select_query(self, query, fetch=None):
		'Queries the database with the given PostreSQL query'
		cur = self.conn.cursor()
		cur.execute(query)
		if fetch == 1:
			results = cur.fetchone()
		else:
			results = cur.fetchall()
		cur.close()
		return results
	def insert_query(self, query, returning=False):
		'Inserts information into the PostgreSQL database'
		cur = self.conn.cursor()
		cur.execute(query)
		if returning:
			results = cur.fetchone()
		self.conn.commit()
		cur.close()
		if returning:
			return results
	def populate_table_names_and_fields(self):
		'Populates a dictionary for table names and fields'
		self.tables = {}
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'"
		for table in self.select_query(query):
			query = "SELECT column_name FROM information_schema.columns WHERE table_name = '%s'" % (table)
			fields = []
			for column in self.select_query(query):
				fields.append(column[0])
			self.tables[table[0]] = tuple(fields)
	def close_connection(self):
		'Closes the connection to the database'
		try:
			self.conn.close()
		except:
			pass