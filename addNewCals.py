#!/usr/bin/env python

#############################################################################
#	addNewCals.py															#
#																			#
#	Author:		Adam Baker (ambaker@usgs.gov)								#
#	Date:		2016-05-13													#
#	Version:	1.1.8														#
#																			#
#	Purpose:	Allows for quicker implementation of a database				#
#############################################################################

import argparse
import caluser
import database
import glob
import os
import psycopg2
import struct

from obspy.core import UTCDateTime

debug = True

#initializes the variables
net  = ''
sta  = ''
date = ''
year = ''
jday = ''
loc  = ''
locs = []
chan = ''
chans= []

# def main():
# 	#main logic sequence
# 	global conn
# 	conn = psycopg2.connect("dbname='cals' user='caluser' host='136.177.121.26' password='" + caluser.password() + "'")
# 	arguments = get_arguments()
# 	set_arguments(arguments)
# 	# process_cals()
# 	conn.close()

def main():
	'Main logic sequence'
	global caldb
	caldb = database.Database('cals','caluser','136.177.121.26',caluser.password())
	arguments = get_arguments()
	set_arguments(arguments)
	process_calibrations()
	caldb.close_connection()
	

def get_arguments():
	'Parses the command line arguments'
	parser = argparse.ArgumentParser(description='Code to compare data availability')

	#sets flag for the network
	parser.add_argument('-n', action = "store",dest="net", default = "NN", help="       Network to check: NN", type = str, required = True)

	#sets flag for the station (optional)
	parser.add_argument('-s', action = "store",dest="sta", default = "SSSS", help="       Station to check: SSSS", type = str, required = True)

	#sets flag for the output filename
	parser.add_argument('-d', action = "store",dest="date", default = "2015,001", help="Date to check: YYYY(,DDD)", type = str, required = True)

	parserval = parser.parse_args()
	return parserval

def set_arguments(arguments):
	'Globally sets the arguments received from get_arguments()'
	global net, sta, date, year, jday
	net = arguments.net
	sta = arguments.sta
	date = arguments.date
	year, jday = date.split(',')

def process_calibrations():
	'Processes the cals, inserting them into the database when necessary'
	global loc, chan
	filepaths = glob.glob('/msd/%s_%s/%s/%s/*[BL]HZ*.seed' % (net, sta, year, jday))
	for filepath in filepaths:
		for calibration in get_calibrations(filepath):
			loc, chan = filepath.split('/')[-1].split('.')[0].split('_')
			add_calibration()

def add_calibration():
	'Adds the calibration to the database if not a duplicate'
	check_location()
	getSensorid()
	# check_calibration()

def check_location():
	'Adds the location to the database if not a duplicate'
	query = """SELECT tbl_stations.pk_id, tbl_locations.pk_id FROM tbl_networks
					JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
					JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
				WHERE network = '%s' AND station_name = '%s' AND location = '%s'
			""" % (net, sta, loc)
	stationid = caldb.select_query(query)
	if not stationid:
		#if location is not found
		query = """INSERT INTO tbl_locations ('fk_stationid','location')
					VALUES (%s, %s) RETURNING pk_id""" % (stationid[0][0], loc)
		locationid = caldb.insert_query(query)[0][0]
		return locationid
	return stationid[0][1]

def check_calibration():
	'Adds the calibration to the database if not a duplicate'
	query = """SELECT tbl_stations.pk_id, tbl_locations.pk_id FROM tbl_networks
					JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
					JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
					JOIN tbl_sensors
				WHERE network = '%s' AND station_name = '%s' AND location = '%s'
			""" % (net, sta, loc)

	# filepath = glob.glob('/xs[01]/seed/' + net + '_' + sta + '/')[0]
	# try:
	# 	filepath = glob.glob(filepath + year + '/' + year + '_' + jday + '_' + net + '_' + sta + '/')[0]
	# 	filepaths = glob.glob(filepath + '*[BL]HZ*')
	# except:
	# 	filepaths = []
	# for filepath in filepaths:
	# 	for calibration in getCalibrations(filepath):
	# 		#Checks to see if there is a calibration for this day (e.g. it was not passed an empty list)
	# 		if calibration != []:
	# 			global loc, chan
	# 			locchan = filepath.split('/')[-1].split('_')
	# 			if locchan[0][:3].isalpha():
	# 				chan = locchan[0][:3]
	# 			elif locchan[0].isdigit() and locchan[1][:3].isalpha():
	# 				loc = locchan[0]
	# 				chan = locchan[1][:3]
	# 			cur = conn.cursor()
	# 			cal = calibration
	# 			#Processes a step calibration
	# 			if cal['type'] == 300:
	# 				query = "INSERT INTO tbl_300 (fk_sensorid, type, startdate, flags, num_step_cals, step_duration, interval_duration, amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['num_step_cals']) + ', ' +  str(cal['step_duration']) + ', ' +  str(cal['interval_duration']) + ', ' +  str(cal['amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
	# 				if debug:
	# 					print '\tStep cal found:', net, sta.ljust(4), cal['startdate'].replace('T',' ').split('.')[0]
	# 			#Processes a sine calibration
	# 			if cal['type'] == 310:
	# 				query = "INSERT INTO tbl_310 (fk_sensorid, type, startdate, flags, cal_duration, signal_period, amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['cal_duration']) + ', ' +  str(cal['signal_period']) + ', ' +  str(cal['amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
	# 				if debug:
	# 					print '\tSine cal found:', net, sta.ljust(4), cal['startdate'].replace('T',' ').split('.')[0]
	# 			#Processes a random calibration
	# 			if cal['type'] == 320:
	# 				query = "INSERT INTO tbl_320 (fk_sensorid, type, startdate, flags, cal_duration, ptp_amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['cal_duration']) + ', ' +  str(cal['ptp_amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
	# 				if debug:
	# 					print '\tRand cal found:', net, sta.ljust(4), cal['startdate'].replace('T',' ').split('.')[0]
	# 			cur.execute(query)
	# 			conn.commit()
	# 			cur.close()



def get_calibrations(file_name):
	'Attempts to retrieve calibrations by looking for calibration blockettes (300, 310, 320)'
	#mostly written by Adam Ringler
	calibrations = []
	#read the first file and get the record length from blockette 1000
	fh = open(file_name, 'rb')
	record = fh.read(256)
	index = struct.unpack('>H', record[46:48])[0]
	file_stats = os.stat(file_name)
	try:
		record_length = 2 ** struct.unpack('>B', record[index+6:index+7])[0]
		#get the total number of records
		total_records = file_stats.st_size / record_length
		#now loop through the records and look for calibration blockettes
		for rec_idx in xrange(0, total_records):
			fh.seek(rec_idx * record_length,0)
			record = fh.read(record_length)
			next_blockette = struct.unpack('>H', record[46:48])[0]
			while next_blockette != 0:
				index = next_blockette
				blockette_type, next_blockette = struct.unpack('>HH', record[index:index+4])
				if blockette_type in (300, 310, 320, 390):
					year,jday,hour,minute,sec,_,tmsec,_,calFlags,duration = struct.unpack('>HHBBBBHBBL', record[index+4:index+20])
					stime = UTCDateTime(year=year,julday=jday,hour=hour,minute=minute,second=sec)
					if blockette_type == 300:
						#blockette for step cals
						numStepCals,_,_,intervalDuration,amplitude,calInput = struct.unpack('>BBLLf3s', record[index+14:index+31])
						calibrations.append({'type': 300, 'startdate': str(stime), 'flags': calFlags, 'num_step_cals': numStepCals, 'step_duration': duration, 'interval_duration': intervalDuration, 'amplitude': amplitude, 'channel': calInput})
					if blockette_type == 310:
						#blockette for sine cals
						signalPeriod,amplitude,calInput = struct.unpack('>ff3s',record[index+20:index+31])
						calibrations.append({'type': 310, 'startdate': str(stime), 'flags': calFlags, 'cal_duration': duration, 'signal_period': signalPeriod, 'amplitude': amplitude, 'channel': calInput})
					if blockette_type == 320:
						#blockette for psuedorandom cals
						amplitude,calInput = struct.unpack('>f3s', record[index+20:index+27])
						calibrations.append({'type': 320, 'startdate': str(stime), 'flags': calFlags, 'cal_duration': duration, 'ptp_amplitude': amplitude, 'channel': calInput})
					if blockette_type == 390:
						#blockette for generic cals, currently unused
						amplitude,calInput = struct.unpack('>f3s', record[index+20:index+27])
						calibrations.append({'type': 390, 'startdate': str(stime), 'flags': calFlags, 'duration': duration, 'amplitude': amplitude, 'channel': calInput})
						if debug:
							print 'Generic cal:', net, sta, cal['startdate']
	except:
		pass
	fh.close()
	return calibrations

def getSensorid():
	#queries for a list of the networks and populates a dictionary
	query = """SELECT tbl_sensors.pk_id AS sensor, startdate FROM tbl_networks
					JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
					JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
					JOIN tbl_sensors ON tbl_sensors.fk_locationid = tbl_locations.pk_id
				WHERE network = '%s' AND station_name = '%s' AND location = '%s'
			""" % (net, sta, loc)
	sensorid = findAppropriateSensorID(caldb.select_query(query))
	print 'SENSOR ID', sensorid
	return sensorid

def queryDatabase(query):
	#assists in querying the database
	cur = conn.cursor()
	cur.execute(query)
	results = cur.fetchall()
	cur.close()
	return results

def findAppropriateSensorID(sensorIDsDates):
	#returns the primary key of the appropriate sensor
	date = str(UTCDateTime(year + '-' + jday))
	dates = [date]
	for sensorid, epochstart in sensorIDsDates:
		if epochstart <= date:
			dates.append(epochstart)
	dates.sort()
	for sensorid, epochstart in sensorIDsDates:
		if dates[dates.index(date) - 1] == epochstart:
			return sensorid

if __name__ == "__main__":
	main()