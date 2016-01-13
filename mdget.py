#!/usr/bin/env python
import socket
import argparse
import sys
from obspy.core import UTCDateTime
from time import sleep


#######################################################################
#Code for Mdget in python
#By Adam Ringler
#This is a python version of mdget.  It uses Dave Ketchum's metadata
#server and parse out the important information
#
#Here are the functions
#getString()
#modifyDateTime()
#parseresp()
#getvalue()
#
#######################################################################

#Variables that will not change often
host = "137.227.224.97"
port = 2052
maxslept = 60 / 0.05
maxblock = 2*10240

def getString(net,sta,loc,chan):
#This function formats the string to be used in mdget
	debugmdgetString = False
#No wild carding of networks
	mdgetString = "-s " + net

#Now we split with * and append a . in front to allow for wild cards
	if "*" in sta:
		mdgetString += ".*".join(sta.split("*"))		
	else:
		mdgetString += sta.ljust(5,'-')
	if "*" in chan:
		mdgetString += ".*".join(chan.split("*"))
	else:
		mdgetString += chan
	if loc == "*":
		mdgetString += ".*".join(loc.split("*"))
	else:	
		mdgetString += loc.ljust(2,'-')
	if debugmdgetString:
		print 'Here is the string: ' + mdgetString
	return mdgetString


def modifyDateTime(timeIn):
#This function modifies a UTCDateTime and returns the string for
#mdget so that it can be used
	debugmodifyDateTime = False
	
	timeStringMdget = str(timeIn.year) + '/' 
	timeStringMdget += str(timeIn.month).zfill(2) + '/'
	timeStringMdget += str(timeIn.day).zfill(2) + '-'
	timeStringMdget += str(timeIn.hour).zfill(2) + ":"
	timeStringMdget += str(timeIn.minute).zfill(2) + ":"
	timeStringMdget += str(timeIn.second).zfill(2) 
	
	if debugmodifyDateTime:
		print 'Here is the string in: ' + timeIn.ctime()
		print 'Here is the string out: ' + timeStringMdget
	return timeStringMdget

def parseresp(data):
#This function parses the data and gets out the key values for 
#the station
	debugParseresp = False
#Here we parse the 
	datalessobject = {}
	if debugParseresp:
		print data[0]
#Get the network as well as the other parameters that are easy
	datalessobject['network'] = getvalue("* NETWORK",data)
	datalessobject['station'] = getvalue("* STATION",data)
	datalessobject['channel'] = getvalue("* COMPONENT",data)
	datalessobject['location'] = getvalue("* LOCATION",data)
	datalessobject['start date'] = getvalue("* EFFECTIVE",data)
	datalessobject['end date'] = getvalue("* ENDDATE",data)
	datalessobject['input unit'] = getvalue("* INPUT UNIT",data)
	datalessobject['output unit'] = getvalue("* OUTPUT UNIT",data)
	datalessobject['description'] = getvalue("* DESCRIPTION",data)
	datalessobject['sampling rate'] = getvalue("* RATE (HZ)",data)
	datalessobject['latitude'] = getvalue("* LAT-SEED",data)
	datalessobject['longitude'] = getvalue("* LONG-SEED",data)
	datalessobject['elevation'] = getvalue("* ELEV-SEED",data)
	datalessobject['depth'] = getvalue("* DEPTH",data)
	datalessobject['dip'] = getvalue("* DIP",data)
	datalessobject['azimuth'] = getvalue("* AZIMUTH",data)	
	datalessobject['instrument type'] = getvalue("* INSTRMNTTYPE",data)
	datalessobject['sensitivity'] = getvalue("* SENS-SEED",data)
	
#Now we need to get the poles and zeros
	nzeros = int(getvalue("ZEROS",data))
	npoles = int(getvalue("POLES",data))
	zerosindex = data.index("* ****") + 3
	polesindex = zerosindex + nzeros + 1
	poles = []
	zeros = []
	if debugParseresp:
		print 'Here is the index of the zeros: ' + str(zerosindex)
		print 'Here is the index of the poles: ' + str(polesindex)
		print 'Here is the first zero: ' + str(data[zerosindex])
		print 'Here is the first pole: ' + str(data[polesindex])
		print 'Here is the number of zeros: ' + str(nzeros)
		print 'Here is the number of poles: ' + str(npoles)

#Lets go through the zeros
	for zstart in range(0,nzeros):
#Remove all of the extra white spaces
		curzero = ' '.join(data[zerosindex + zstart].split())
		if debugParseresp:
			print 'Here is what is coming in: ' + curzero
#Split into two		
		curzero = curzero.split()
		curzero = float(curzero[0]) + 1j*float(curzero[1])
		zeros.append(curzero)
		if debugParseresp:
			print 'Here is another zero:' + str(curzero)
	datalessobject['zeros'] = zeros
#Lets start dealing with the poles
	for pstart in range(0,npoles):
		curpole = ' '.join(data[polesindex + pstart].split())
		if debugParseresp:
			print 'Here is what is coming in: ' + curpole
#Split into two		
		curpole = curpole.split()
		curpole = float(curpole[0]) + 1j*float(curpole[1])
		poles.append(curpole)
		if debugParseresp:
			print 'Here is another pole:' + str(curpole)
	datalessobject['poles'] = poles	

	return  datalessobject

def getvalue(strSearch, data):
#This is a helper function to not call the search function a bunch
#Currently this function only deals with 1 parameter so it needs to be
#modified to deal with poles and zeros
	value = []
	value = [s for s in data if strSearch in s]
	value = (value[0].replace(strSearch,'')).strip()
	return value


#Here is the start of the main program.  Some of this will eventually
#need to be put into functions

#Setup basic parser
#This should all possibly be included in a function
parser = argparse.ArgumentParser(description='Code to get dataless from mdget')

parser.add_argument('-s','--station', type = str, action = "store", dest="station", \
default = "*", help="Name of the station of interest: SSSSS", required = False)

parser.add_argument('-l','--location', type = str, action = "store", dest="location", \
default = "*", help="Name of the location of interest: LL", required = False)

parser.add_argument('-n','--network', action = "store",dest="network", \
default = "*", help="Name of the network of interest: NN", type = str, required = True)

parser.add_argument('-c','--channel', action = "store",dest="channel", \
default ="*", help="Name of the channel of interest: CCC", type = str, required = False)

parser.add_argument('-d','--debug',action = "store_true",dest="debug", \
default = False, help="Run in debug mode")

parser.add_argument('-t','--time',type = str, action = "store", dest= "time", \
default = "", required = False, help="Time of Epoch: YYYY-DDD")

parser.add_argument('-o','--output',type = str,action = "store", dest = "output", \
default = "description", help="Name of parsed value of interest", required = False)

parserval = parser.parse_args()

if parserval.debug:
	print 'Running in debug mode'
	debug = True
else:
	debug = False

#If we have a time we want to pull it out and use it for the epoch
if parserval.time:

	try:
		stime = UTCDateTime(parserval.time.split('-')[0] + "-" + \
			parserval.time.split('-')[1] + "T00:00:00.0") 
	except:
		print 'Problem reading epoch'
		sys.exit(0)
	if debug:
		print 'Here is the epoch time of interest:' + stime 	

#We need a function to parse the imput string for various epochs
importstring = getString(parserval.network,parserval.station, \
	parserval.location,parserval.channel)


#Open the socket and connect
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))

#Set blocking we will want a different timeout approach
s.setblocking(maxslept)

if parserval.time:
	importstring += ' -b ' + modifyDateTime(stime)

importstring +=  " -c r\n"
if debug:
	print importstring

s.sendall(importstring)

#Now lets get the data
getmoredata = True

data=''
while getmoredata:
#Pulling the request data and adding it into one big string
	data += s.recv(maxblock)
	if "* <EOR>" in data:
		if debug:
			print 'Found the end of the output'
		getmoredata = False
	else:
		if debug:
			print 'Okay getting more data'
	if 'no channels found' in data:
		print 'No channels found\n'
	sleep(0.05)
s.close()

#Splitting the data by EOE into a list
data = data.split('* <EOE>')
data.pop()
for curepoch in data:
	if debug:
		print 'Here is a new epoch'
		print curepoch

#Here we split the current epoch by line and pull out the important information
	parseddata = parseresp(curepoch.split('\n'))
	if debug:
		print 'Here is the station we are at: ' + parseddata['station']
	print parseddata['network'] + ', ' + parseddata['station'] + ', ' + \
		parseddata['location'] + ', ' + parseddata['channel'] + ', ' + \
		str(parseddata[parserval.output])


#Need to include the main part of the program and make things pretty
