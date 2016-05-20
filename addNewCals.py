#!/usr/bin/env python

#############################################################################
#	addNewCals.py															#
#																			#
#	Author:		Adam Baker (ambaker@usgs.gov)								#
#	Date:		2016-05-20													#
#	Version:	1.7.15														#
#																			#
#	Purpose:	Allows for quicker implementation of a database				#
#############################################################################

import argparse
import caluser
import commands
import database
import datalesstools
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
dataless = None

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
	try:
		filepaths = glob.glob(find_appropriate_filepath())
		for filepath in filepaths:
			for calibration in get_calibrations(filepath):
				loc, chan = filepath.split('/')[-1].split('.')[0].split('_')
				add_calibration(calibration)
	except:
		pass

def find_appropriate_filepath():
	'Finds the first available stationday seed filepath'
	msdPath = '/msd/%s_%s/%s/%s/' % (net, sta, year, jday)
	if os.path.exists(msdPath):
		return msdPath + '*[BL]HZ*.seed'
	tr1Path = '/tr1/telemetry_days/%s_%s/%s/%s_%s/' % (net, sta, year, year, jday)
	if os.path.exists(tr1Path):
		return tr1Path + '*[BL]HZ*.seed'
	xs0Path = '/xs0/seed/%s_%s/%s/%s_%s_%s_%s/' % (net, sta, year, year, jday, net, sta)
	if os.path.exists(xs0Path):
		return xs0Path + '*[BL]HZ*.seed'
	xs1Path = '/xs1/seed/%s_%s/%s/%s_%s_%s_%s/' % (net, sta, year, year, jday, net, sta)
	if os.path.exists(xs1Path):
		return xs1Path + '*[BL]HZ*.seed'

def add_calibration(cal):
	'Adds the calibration to the database if not a duplicate'
	networkid = check_network()
	stationid = check_station(networkid)
	locationid = check_location(stationid)
	sensorid = check_sensor(locationid)
	check_calibration(cal, sensorid)

def check_network():
	'Adds the network to the database if not a duplicate'
	query = """SELECT pk_id FROM tbl_networks
				WHERE network = '%s'""" % (net)
	networkid = caldb.select_query(query, 1)
	if networkid:
		return networkid[0]
	if not networkid:
		query = """INSERT INTO tbl_networks (network)
					VALUES (%s) RETURNING pk_id""" % (net)
		networkid = caldb.insert(query, True)
		return networkid

def check_station(networkid):
	'Adds the station to the database if not a duplicate'
	query = """SELECT tbl_stations.pk_id FROM tbl_networks
					JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
				WHERE network = '%s' AND station_name = '%s'""" % (net, sta)
	stationid = caldb.select_query(query, 1)
	if stationid:
		return stationid[0]
	if not stationid:
		query = """INSERT INTO tbl_stations (fk_networkid, station_name)
					VALUES (%s, '%s') RETURNING pk_id""" % (networkid, sta)
		stationid = caldb.insert_query(query, True)
		return stationid

def check_location(stationid):
	'Adds the location to the database if not a duplicate'
	query = """SELECT tbl_locations.pk_id FROM tbl_networks
					JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
					JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
				WHERE network = '%s' AND station_name = '%s' AND location = '%s'""" % (net, sta, loc)
	locationid = caldb.select_query(query, 1)
	if locationid:
		return locationid[0]
	if not locationid:
		query = """INSERT INTO tbl_locations (fk_stationid, location)
					VALUES (%s, %s) RETURNING pk_id""" % (stationid, loc)
		locationid = caldb.insert_query(query, True)
		return locationid

def check_sensor(locationid):
	'Adds the sensor to the database if not a duplicate'
	global dataless
	instrumentid = 0
	if not dataless:
		dataless = datalesstools.getDataless(net + sta)
	for station in dataless.stations:
		if station[0].blockette_type == 50 and station[0].station_call_letters == sta:
			for blockette in station:
				if blockette.blockette_type == 52 and blockette.location_identifier == loc and blockette.channel_identifier == chan and blockette.start_date <= UTCDateTime(year + jday + 'T23:59:59.999999Z') <= blockette.end_date:
					startdate = blockette.start_date
					enddate = blockette.end_date
					instrumentid = blockette.instrument_identifier
					break
	query = """SELECT tbl_sensors.pk_id FROM tbl_networks
					JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
					JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
					JOIN tbl_sensors ON tbl_sensors.fk_locationid = tbl_locations.pk_id
				WHERE network = '%s' AND station_name = '%s' AND location = '%s' AND startdate <= '%s' AND enddate >= '%s'""" % (net, sta, loc, startdate, enddate)
	sensorid = caldb.select_query(query, 1)
	if sensorid:
		return sensorid[0]
	if not sensorid:
		dictB031, dictB033, dictB034 = getDictionaries()
		sensorName = fetchInstrument(dictB033, instrumentid)
		query = """INSERT INTO tbl_locations (fk_locationid, sensor, startdate, enddate)
					VALUES (%s, %s, %s, %s) RETURNING pk_id""" % (locationid, sensorName, startdate, enddate)
		sensorid = caldb.insert_query(query, True)
		return sensorid

def check_calibration(cal, sensorid):
	'Adds the calibration to the database if not a duplicate'
	# query = """SELECT tbl_stations.pk_id, tbl_locations.pk_id FROM tbl_networks
	# 				JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
	# 				JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
	# 				JOIN tbl_sensors ON tbl_sensors.fk_locationid = tbl_locations.pk_id
	# 			WHERE network = '%s' AND station_name = '%s' AND location = '%s'
	# 		""" % (net, sta, loc)
	query = """SELECT tbl_%s.pk_id, tbl_networks.network, tbl_stations.station_name, tbl_locations.location, tbl_sensors.sensor, tbl_%s.startdate, tbl_%s.channel, tbl_%s.%s FROM tbl_networks
		JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
		JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
		JOIN tbl_sensors ON tbl_sensors.fk_locationid = tbl_locations.pk_id
		JOIN tbl_%s ON tbl_%s.fk_sensorid = tbl_sensors.pk_id
		WHERE network = '%s' AND station_name = '%s' AND tbl_%s.startdate = '%s' AND location = '%s' AND channel = '%s' AND %s = %s"""
	if cal['type'] == 300:
		query %= (cal['type'], cal['type'], cal['type'], cal['type'], 'step_duration', cal['type'], cal['type'], net, sta, cal['type'], cal['startdate'], loc, cal['channel'], 'step_duration', cal['step_duration'])
	else:
		query %= (cal['type'], cal['type'], cal['type'], cal['type'], 'cal_duration', cal['type'], cal['type'], net, sta, cal['type'], cal['startdate'], loc, cal['channel'], 'cal_duration', cal['cal_duration'])
	if not caldb.select_query(query):
		#if the cal does not exist in the database
		if cal['type'] == 300:
			query = """INSERT INTO tbl_%s (fk_sensorid, type, startdate, flags, num_step_cals, step_duration, interval_duration, amplitude, channel) VALUES (%s, '%s', '%s', '%s', %s, %s, %s, %s, '%s')""" % (cal['type'], sensorid, cal['type'], cal['startdate'], cal['flags'], cal['num_step_cals'], cal['step_duration'], cal['interval_duration'], cal['amplitude'], cal['channel'])
		if cal['type'] == 310:
			query = """INSERT INTO tbl_%s (fk_sensorid, type, startdate, flags, cal_duration, signal_period, amplitude, channel) VALUES (%s, '%s', '%s', '%s', %s, %s, %s, '%s')""" % (cal['type'], sensorid, cal['type'], cal['startdate'], cal['flags'], cal['cal_duration'], cal['signal_period'], cal['amplitude'], cal['channel'])
		if cal['type'] == 320:
			query = """INSERT INTO tbl_%s (fk_sensorid, type, startdate, flags, cal_duration, ptp_amplitude, channel) VALUES (%s, '%s', '%s', '%s', %s, %s, '%s')""" % (cal['type'], sensorid, cal['type'], cal['startdate'], cal['flags'], cal['cal_duration'], cal['ptp_amplitude'], cal['channel'])
		caldb.insert_query(query)
		print '\tCal detected and inserted', net, sta, loc, year, jday, cal['startdate'].split('T')[-1].split('.')[0]

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

# def getSensorid():
# 	'Queries for a list of the networks and populates a dictionary'
# 	query = """SELECT tbl_sensors.pk_id AS sensor, startdate FROM tbl_networks
# 					JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
# 					JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
# 					JOIN tbl_sensors ON tbl_sensors.fk_locationid = tbl_locations.pk_id
# 				WHERE network = '%s' AND station_name = '%s' AND location = '%s'
# 			""" % (net, sta, loc)
# 	sensorid = findAppropriateSensorID(caldb.select_query(query))
# 	return sensorid

# def queryDatabase(query):
# 	'Assists in querying the database'
# 	cur = conn.cursor()
# 	cur.execute(query)
# 	results = cur.fetchall()
# 	cur.close()
# 	return results

# def findAppropriateSensorID(sensorIDsDates):
# 	'Returns the primary key of the appropriate sensor'
# 	date = str(UTCDateTime(year + jday + 'T235959.999999'))
# 	dates = [date]
# 	for sensorid, epochstart in sensorIDsDates:
# 		if epochstart <= date:
# 			dates.append(epochstart)
# 	dates.sort()
# 	for sensorid, epochstart in sensorIDsDates:
# 		if dates[dates.index(date) - 1] == epochstart:
# 			return sensorid

def getDictionaries():
	b031, b033, b034 = parseRDSEEDAbbreviations(commands.getstatusoutput(formRDSEEDCommand(net, sta))[-1])
	return b031, b033, b034

def formRDSEEDCommand(net, sta):
	netsta = net + '_' + sta
	path = ''
	if os.path.exists('/msd/%s_%s/' % (net, sta)):
		path = '/msd/%s_%s/' % (net, sta)
	elif os.path.exists('/xs0/seed/' + netsta):
		path = '/xs0/seed/' + netsta
	elif os.path.exists('/xs1/seed/' + netsta):
		path = '/xs1/seed/' + netsta
	elif os.path.exists('/tr1/telemetry_days/' + netsta):
		path = '/tr1/telemetry_days/' + netsta
	return 'rdseed -f ' + '%s -g /APPS/metadata/SEED/%s.dataless -a' % (filepath, net)
	
def globMostRecent(filepath):
	paths = glob.glob(filepath + '/*')
	pathsTemp = []
	for path in paths:
		if len(path.split('/')[-1]) <= 16 and 'SAVE' not in path:
			pathsTemp.append(path)
	return max(pathsTemp)

def parseRDSEEDAbbreviations(output):
	b031 = []
	b033 = []
	b034 = []
	for group in output.split('#\t\t\n'):
		if 'B031' == group[:4]:
		# 	dictionary = {}
		# 	for line in group.strip().split('\n'):
		# 		if 'B031F03' in line:
		# 			dictionary['comment code id'] = int(line.split('  ')[-1].strip())
		# 		elif 'B031F04' in line:
		# 			dictionary['comment class code'] = line.split('  ')[-1].strip()
		# 		elif 'B031F05' in line:
		# 			dictionary['comment text'] = line.split('  ')[-1].strip()
		# 		elif 'B031F06' in line:
		# 			dictionary['comment units'] = line.split('  ')[-1].strip()
		# 	b031.append(dictionary)
			pass
		elif 'B033' == group[:4]:
			dictionary = {}
			for line in group.strip().split('\n'):
				if 'B033F03' in line:
					dictionary['description key code'] = int(line.split('  ')[-1].strip())
				elif 'B033F04' in line:
					dictionary['abbreviation description'] = line.split('  ')[-1].strip()
			b033.append(dictionary)
		# elif 'B034' == group[:4]:
		# 	dictionary = {}
		# 	for line in group.strip().split('\n'):
		# 		if 'B034F03' in line:
		# 			dictionary['unit code'] = int(line.split('  ')[-1].strip())
		# 		elif 'B034F04' in line:
		# 			dictionary['unit name'] = line.split('  ')[-1].strip()
		# 		elif 'B034F05' in line:
		# 			dictionary['unit description'] = line.split('  ')[-1].strip()
		# 	b034.append(dictionary)
	return b031, b033, b034

# def fetchComment(dictB031, value):
# 	'Blockette 31, used for describing comments'
# 	for comment in dictB031:
# 		if value == comment['comment code id']:
# 			return [comment['comment text'], comment['comment units'], comment['comment class code']]
# 	return ['No comments found', 'N/A', '0']

def fetchInstrument(dictB033, value):
	'Blockette 33, used for describing instruments'
	for instrument in dictB033:
		if value == instrument['description key code']:
			return instrument['abbreviation description']
	return 'No instrument found'

# def fetchUnit(dictB034, value):
# 	'Blockette 34, used for describing units'
# 	for unit in dictB034:
# 		if value == unit['unit code']:
# 			return [unit['unit name'], unit['unit description']]
# 	return ['None', 'No units found']

if __name__ == "__main__":
	main()