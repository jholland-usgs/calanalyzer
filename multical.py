#!/usr/bin/env python

import argparse
import commands
import glob

from multiprocessing import Pool
from obspy.core import UTCDateTime

now = UTCDateTime.now()

net = ''
network = ''
sta = ''
station = ''
bdate = ''
byear = ''
bjday = ''
edate = ''
eyear = ''
ejday = ''

def main():
	arguments = getArguments()
	setArguments(arguments)
	pool = Pool(20)
	checkDates()
	print 'DONE'

def getArguments():
	#This function parses the command line arguments
	parser = argparse.ArgumentParser(description='Code to compare data availability')

	#Sets flag for the network
	parser.add_argument('-n', action = "store",dest="net", default = "[CGINU][UTCWES]", help="       Network to check: NN", type = str, required = False)

	#Sets flag for the station (optional)
	parser.add_argument('-s', action = "store",dest="sta", default = "*", help="       Station to check: SSSS", type = str, required = False)

	#Sets flag for the output filename
	parser.add_argument('-b', action = "store",dest="bdate", default = "1972,181", help="Date to check: YYYY(,DDD)", type = str, required = False)

	#Sets flag for the output filename
	parser.add_argument('-e', action = "store",dest="edate", default = str(now.year) + ',' + str(now.julday), help="Date to check: YYYY(,DDD)", type = str, required = False)

	parserval = parser.parse_args()
	return parserval

def setArguments(arguments):
	global net, sta, bdate, byear, bjday, edate, eyear, ejday
	net = arguments.net.upper()
	sta = arguments.sta.upper()
	bdate = arguments.bdate
	byear, bjday = bdate.split(',')
	edate = arguments.edate
	eyear, ejday = edate.split(',')
	print net, sta, bdate, byear, bjday, edate, eyear, ejday

def checkDates():
	pool = Pool(20)
	netstas = glob.glob('/xs[01]/seed/' + net + '_' + sta)
	pool.map(checkNetsta, netstas)

def checkNetsta(netsta):
	global network, station
	network, station = netsta.split('/')[-1].split('_')
	for year in xrange(int(byear), int(eyear) + 1):
		print str(UTCDateTime.now()).split('.')[0].replace('T',' '), 'Checking', network, station.ljust(4), year, 'for calibrations'
		if year == int(byear) and year != int(eyear):
			for day in xrange(int(bjday), 366 + 1):
				output = commands.getstatusoutput('python /home/ambaker/calanalyzer/addNewCals.py -n ' + network + ' -s ' + station + ' -d ' + str(year) + ',' + str(day).zfill(3))[1]
				print output
		elif int(byear) < year < int(eyear):
			for day in xrange(1, 366 + 1):
				ouput = commands.getstatusoutput('python /home/ambaker/calanalyzer/addNewCals.py -n ' + network + ' -s ' + station + ' -d ' + str(year) + ',' + str(day).zfill(3))[1]
				print output
		elif year == int(eyear) and year != int(byear):
			for day in xrange(1, int(ejday) + 1):
				output = commands.getstatusoutput('python /home/ambaker/calanalyzer/addNewCals.py -n ' + network + ' -s ' + station + ' -d ' + str(year) + ',' + str(day).zfill(3))[1]
				print output
		elif year == int(byear) == int(eyear):
			for day in xrange(int(bjday), int(ejday) + 1):
				output = commands.getstatusoutput('python /home/ambaker/calanalyzer/addNewCals.py -n ' + network + ' -s ' + station + ' -d ' + str(year) + ',' + str(day).zfill(3))[1]
				print output

main()