#!/usr/bin/env python
#############################################################################
#	datalesstools.py														#
#																			#
#	Author:		Adam Baker (ambaker@usgs.gov)								#
#	Date:		2016-05-17													#
#	Version:	1.0.1														#
#																			#
#	Purpose:	Returns the dataless for a given network or station			#
#############################################################################

#imports
import os
import sys
from aslparser import Parser as aslParser
from obspy.io.xseed import Parser

#variables
netDatalessPath = '/APPS/metadata/SEED/'
staDatalessPath = '/dcc/metadata/dataless/'

#functions
def getDataless(netsta):
	#the function that returns the raw dataless
	net = netsta[:2].upper()
	parsedDataless = Parser(netDatalessPath + net + '.dataless')
	return parsedDataless

def getStationDataless(netsta):
	#the function that returns the dataless for a given station
	net = netsta[:2].upper()
	sta = netsta[2:].upper()
	netsta = '_'.join([net,sta])
	if os.path.exists(staDatalessPath + 'DATALESS.' + netsta + '.seed'):
		station = []
		parsedDataless = aslParser(staDatalessPath + 'DATALESS.' + netsta + '.seed')
		for blockette in parsedDataless.stations:
			station.extend(blockette)
		return station
	else:
		parsedDataless = Parser(netDatalessPath + net + '.dataless')
		if len(netsta) > 2:
			sta = netsta[2:].upper()
			for station in parsedDataless.stations:
				for blockette in station:
					if blockette.id == 50:
						if blockette.station_call_letters == sta:
							return station

def getNetworkDataless(netsta):
	#the function that returns the dataless for a given network
	net = netsta[:2].upper()
	parsedDataless = Parser(netDatalessPath + net + '.dataless')
	return parsedDataless.stations

def forceStationDataless(netsta):
	#the function that returns the dataless for a given station
	net = netsta[:2].upper()
	sta = netsta[2:].upper()
	netsta = '_'.join([net,sta])
	if os.path.exists(staDatalessPath + 'DATALESS.' + netsta + '.seed'):
		station = []
		parsedDataless = aslParser(staDatalessPath + 'DATALESS.' + netsta + '.seed')
		for blockette in parsedDataless.stations:
			station.extend(blockette)
		return station