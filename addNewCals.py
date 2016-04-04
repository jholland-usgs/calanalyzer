#!/usr/bin/env python

import argparse
import caluser
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
chan = ''

def main():
	#main logic sequence
	global conn
	conn = psycopg2.connect("dbname='cals' user='caluser' host='136.177.121.26' password='" + caluser.password() + "'")
	arguments = getArguments()
	setArguments(arguments)
	processCals()
	conn.close()

def getArguments():
	#this function parses the command line arguments
	parser = argparse.ArgumentParser(description='Code to compare data availability')

	#sets flag for the network
	parser.add_argument('-n', action = "store",dest="net", default = "NN", help="       Network to check: NN", type = str, required = True)

	#sets flag for the station (optional)
	parser.add_argument('-s', action = "store",dest="sta", default = "SSSS", help="       Station to check: SSSS", type = str, required = True)

	#sets flag for the output filename
	parser.add_argument('-d', action = "store",dest="date", default = "2015,001", help="Date to check: YYYY(,DDD)", type = str, required = True)

	parserval = parser.parse_args()
	return parserval

def setArguments(arguments):
	#globally sets the arguments received from the above function
	global net, sta, date, year, jday
	net = arguments.net
	sta = arguments.sta
	date = arguments.date
	year, jday = date.split(',')

def processCals():
	#processes the cals, inserts them into the database
	cals = []
	filepath = glob.glob('/xs[01]/seed/' + net + '_' + sta + '/')[0]
	try:
		filepath = glob.glob(filepath + year + '/' + year + '_' + jday + '_' + net + '_' + sta + '/')[0]
		filepaths = glob.glob(filepath + '*[BL]HZ*')
	except:
		filepaths = []
	for filepath in filepaths:
		for calibration in getCalibrations(filepath):
			#Checks to see if there is a calibration for this day (e.g. it was not passed an empty list)
			if calibration != []:
				global loc, chan
				locchan = filepath.split('/')[-1].split('_')
				if locchan[0][:3].isalpha():
					chan = locchan[0][:3]
				elif locchan[0].isdigit() and locchan[1][:3].isalpha():
					loc = locchan[0]
					chan = locchan[1][:3]
				cur = conn.cursor()
				cal = calibration
				#Check if calibration results already exist in the database
				query = "SELECT tbl_"+cal['type']+""".pk_id, tbl_networks.network,
					           tbl_stations.station_name, tbl_sensors.location, 
					           tbl_"""+cal['type']+".startdate, tbl_"+cal['type']+".channel, tbl_"+cal['type']+""".cal_duration
					    FROM tbl_networks JOIN tbl_stations ON tbl_networks.pk_id = tbl_stations.fk_networkid
						   JOIN tbl_sensors ON tbl_stations.pk_id = tbl_sensors.fk_stationid
						   JOIN tbl_"""+cal['type']+" ON tbl_sensors.pk_id = tbl_"+cal['type']+""".fk_sensorid
					    WHERE network = '""" + str(net) + "' AND station_name = '" + str(sta) + "' AND startdate = '" + str(cal['startdate']) + "' AND location = '" + str(loc) + \
						   "' AND channel = '" + str(cal['channel']) + "' AND cal_duration = '" + str(cal['step_duration']) + "'"
				cur.execute(query)
				if cur.fetchall() == 0:
					#Processes a step calibration
					if cal['type'] == 300:
						query = "INSERT INTO tbl_300 (fk_sensorid, type, startdate, flags, num_step_cals, step_duration, interval_duration, amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['num_step_cals']) + ', ' +  str(cal['step_duration']) + ', ' +  str(cal['interval_duration']) + ', ' +  str(cal['amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
						if debug:
							print '\tStep cal found:', net, sta, cal['startdate'].replace('T',' ').split('.')[0]
					#Processes a sine calibration
					if cal['type'] == 310:
						query = "INSERT INTO tbl_310 (fk_sensorid, type, startdate, flags, cal_duration, signal_period, amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['cal_duration']) + ', ' +  str(cal['signal_period']) + ', ' +  str(cal['amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
						if debug:
							print '\tSine cal found:', net, sta, cal['startdate'].replace('T',' ').split('.')[0]
					#Processes a random calibration
					if cal['type'] == 320:
						query = "INSERT INTO tbl_320 (fk_sensorid, type, startdate, flags, cal_duration, ptp_amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['cal_duration']) + ', ' +  str(cal['ptp_amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
						if debug:
							print '\tRand cal found:', net, sta, cal['startdate'].replace('T',' ').split('.')[0]
					cur.execute(query)
					conn.commit()
				cur.close()



def getCalibrations(file_name):
	#attempts to retrieve calibrations by looking for calibration blockettes (300, 310, 320)
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
		#filler variable assignment
		x = 0
	fh.close()
	return calibrations

def getSensorid():
	#queries for a list of the networks and populates a dictionary
	query = "SELECT pk_id FROM tbl_networks WHERE name = \'" + net + "\'"
	networkid = queryDatabase(query)[0][0]
	#queries for a list of sensors at the given network and station
	query = "SELECT pk_id FROM tbl_stations WHERE fk_networkid = \'" + str(networkid) + "\' AND station_name = \'" + sta + "\'"
	stationid = queryDatabase(query)[0][0]
	#queries for the primary key of the sensor for a given time and location code
	query = "SELECT pk_id AS sensor, startdate FROM tbl_sensors WHERE fk_stationid = \'" + str(stationid) + "\' AND location = \'" + loc + "\'"
	sensors = queryDatabase(query)
	sensorid = findAppropriateSensorID(sensors)
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

main()
