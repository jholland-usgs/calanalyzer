#!/usr/bin/env python

import argparse
import caluser
import glob
import os
import psycopg2
import struct

from obspy.core import UTCDateTime

debug = True

net  = ''
sta  = ''
date = ''
year = ''
jday = ''
loc  = ''
chan = ''

def main():
	global conn
	conn = psycopg2.connect("dbname='cals' user='caluser' host='136.177.121.26' password='" + caluser.password() + "'")
	arguments = getArguments()
	setArguments(arguments)
	processCals()
	conn.close()

def getArguments():
	#This function parses the command line arguments
	parser = argparse.ArgumentParser(description='Code to compare data availability')

	#Sets flag for the network
	parser.add_argument('-n', action = "store",dest="net", default = "NN", help="       Network to check: NN", type = str, required = True)

	#Sets flag for the station (optional)
	parser.add_argument('-s', action = "store",dest="sta", default = "SSSS", help="       Station to check: SSSS", type = str, required = True)

	#Sets flag for the output filename
	parser.add_argument('-d', action = "store",dest="date", default = "2015,001", help="Date to check: YYYY(,DDD)", type = str, required = True)

	parserval = parser.parse_args()
	return parserval

def setArguments(arguments):
	global net, sta, date, year, jday
	net = arguments.net
	sta = arguments.sta
	date = arguments.date
	year, jday = date.split(',')

# def findAllPotentialCals():
# 	cals = []
# 	filepath = glob.glob('/xs[01]/seed/' + net + '_' + sta + '/')[0]
# 	filepath = glob.glob(filepath + year + '/' + year + '_' + jday + '_' + net + '_' + sta + '/')[0]
# 	filepaths = glob.glob(filepath + '*[BL]HZ*')
# 	for filepath in filepaths:
# 		processCals(filepath, getCalibrations(filepath))

def getCalibrations(file_name):
	calibrations = []

	#Read the first file and get the record length from blockette 1000
	fh = open(file_name, 'rb')
	record = fh.read(256)
	index = struct.unpack('>H', record[46:48])[0]
	file_stats = os.stat(file_name)
	try:
		record_length = 2 ** struct.unpack('>B', record[index+6:index+7])[0]
		#Get the total number of records
		total_records = file_stats.st_size / record_length
		#Now loop through the records and look for calibration blockettes
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
						numStepCals,_,_,intervalDuration,amplitude,calInput = struct.unpack('>BBLLf3s', record[index+14:index+31])
						calibrations.append({'type': 300, 'startdate': str(stime), 'flags': calFlags, 'num_step_cals': numStepCals, 'step_duration': duration, 'interval_duration': intervalDuration, 'amplitude': amplitude, 'channel': calInput})
					if blockette_type == 310:
						signalPeriod,amplitude,calInput = struct.unpack('>ff3s',record[index+20:index+31])
						calibrations.append({'type': 310, 'startdate': str(stime), 'flags': calFlags, 'cal_duration': duration, 'signal_period': signalPeriod, 'amplitude': amplitude, 'channel': calInput})
					if blockette_type == 320:
						amplitude,calInput = struct.unpack('>f3s', record[index+20:index+27])
						calibrations.append({'type': 320, 'startdate': str(stime), 'flags': calFlags, 'cal_duration': duration, 'ptp_amplitude': amplitude, 'channel': calInput})
					if blockette_type == 390:
						amplitude,calInput = struct.unpack('>f3s', record[index+20:index+27])
						calibrations.append({'type': 390, 'startdate': str(stime), 'flags': calFlags, 'duration': duration, 'amplitude': amplitude, 'channel': calInput})
						if debug:
							print 'Generic cal:', net, sta, str(stime)
	except:
		x = 0
	fh.close()
	return calibrations

def processCals():
	cals = []
	filepath = glob.glob('/xs[01]/seed/' + net + '_' + sta + '/')[0]
	filepath = glob.glob(filepath + year + '/' + year + '_' + jday + '_' + net + '_' + sta + '/')[0]
	filepaths = glob.glob(filepath + '*[BL]HZ*')
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
				#Processes a step calibration
				if cal['type'] == 300:
					query = "INSERT INTO tbl_300 (fk_sensorid, type, startdate, flags, num_step_cals, step_duration, interval_duration, amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['num_step_cals']) + ', ' +  str(cal['step_duration']) + ', ' +  str(cal['interval_duration']) + ', ' +  str(cal['amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
					if debug:
						print 'Step cal:', net, sta, str(stime)
				#Processes a sine calibration
				if cal['type'] == 310:
					query = "INSERT INTO tbl_310 (fk_sensorid, type, startdate, flags, cal_duration, signal_period, amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['cal_duration']) + ', ' +  str(cal['signal_period']) + ', ' +  str(cal['amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
					if debug:
						print 'Sine cal:', net, sta, str(stime)
				#Processes a random calibration
				if cal['type'] == 320:
					query = "INSERT INTO tbl_320 (fk_sensorid, type, startdate, flags, cal_duration, ptp_amplitude, channel) VALUES (" + str(getSensorid()) + ', ' +  str(cal['type']) + ', ' +  '\'' + cal['startdate'] + '\''  + ', ' +  str(cal['flags']) + ', ' +  str(cal['cal_duration']) + ', ' +  str(cal['ptp_amplitude']) + ', ' +  '\'' + cal['channel'] + '\''  + ")"
					if debug:
						print 'Rand cal:', net, sta, str(stime)
				cur.execute(query)
				conn.commit()
				cur.close()

def getSensorid():
	#Queries for a list of the networks and populates a dictionary
	query = "SELECT pk_id FROM tbl_networks WHERE name = \'" + net + "\'"
	networkid = queryDatabase(query)[0][0]
	query = "SELECT pk_id FROM tbl_stations WHERE fk_networkid = \'" + str(networkid) + "\' AND station_name = \'" + sta + "\'"
	stationid = queryDatabase(query)[0][0]
	query = "SELECT pk_id AS sensor, startdate FROM tbl_sensors WHERE fk_stationid = \'" + str(stationid) + "\' AND location = \'" + loc + "\'"
	sensors = queryDatabase(query)
	sensorid = findAppropriateSensorID(sensors)
	return sensorid

def queryDatabase(query):
	cur = conn.cursor()
	cur.execute(query)
	results = cur.fetchall()
	cur.close()
	return results

def findAppropriateSensorID(sensorIDsDates):
	#Returns the primary key of the appropriate sensor
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