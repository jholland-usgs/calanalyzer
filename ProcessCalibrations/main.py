#!/usr/bin/python
'''
Created on Sep 18, 2015

@author: Nick Falco
'''

from obspy.core import UTCDateTime
import glob
import logging
import os
import re
from src import ParseConfig
from src import ComputeCalibrations
from src import PathData
import psycopg2
from multiprocessing import Pool


'''Connect to the database using the db info read from the config file'''
def connectToDatabase(config):
    print("dbname='" + config.dbname 
        + "' user='" + config.username 
        + "' host='" + config.host 
        + "' password='" + config.password + "'")
    try:
        conn = psycopg2.connect("dbname='" + config.dbname 
                                + "' user='" + config.username 
                                + "' host='" + config.host 
                                + "' password='" + config.password + "'")
    except:
        print ('I am unable to connect to the database')
    return conn
        
''' Returns the set of all networks/stations and calibration dates for a given calibration type'''
def getPathData():
    #Determine the correct table to query based on the calibration type
    durationType = ''
    if(calType == 'sine'):
        calTable = 'tbl_310'
        durationType = 'cal_duration'
    elif(calType == "random"):
        calTable = 'tbl_320'
    elif(calType == "step"):
        calTable = 'tbl_300'
        durationType = 'step_duration'
    #Create cursor used to query the database
    cur = dbconn.cursor()
    #Query the database to retrieve all records for the given calibration type
    query = "SELECT "+calTable+".pk_id, tbl_networks.name, tbl_stations.station_name, tbl_sensors.location, "+calTable+".startdate, "+calTable+".channel, "+calTable+"."+durationType+"""
                 FROM tbl_networks JOIN tbl_stations ON tbl_networks.pk_id = tbl_stations.fk_networkid
                                   JOIN tbl_sensors ON tbl_stations.pk_id = tbl_sensors.fk_stationid
                                   JOIN """+calTable+" ON tbl_sensors.pk_id = "+calTable+""".fk_sensorid
                                   LEFT JOIN """+calTable+"calresults ON "+calTable+".pk_id = "+calTable+"""calresults.fk_calibrationid 
                 WHERE """+calTable+"""calresults is NULL and station_name = 'WCI'
                 ORDER BY startdate DESC"""
    cur.execute(query)
    print (query)
    rows = cur.fetchall()
    
    pathlist = []
    for row in rows:
        pathlist.append(PathData.PathData(cal_id=row[0], network=row[1], station=row[2], location=row[3], date=row[4], channel=row[5], cal_duration=row[6]))
    
    return pathlist
  
def computeNewCal(pathData):      
    if(pathData != None):
        #Calculate equivalent julian calendar day
        julianday = UTCDateTime(pathData.date.year, pathData.date.month, pathData.date.day, 0, 0).julday
        #Build the relative data path
        if(pathData.network == 'US'):
            path = '/xs1/seed/'+pathData.network+'_'+pathData.station+'/'+str(pathData.date.year)+'/'+str(pathData.date.year)+'_'+ str('{0:0=3d}'.format(julianday))+'_'+pathData.network+'_'+pathData.station+'/'
        else:
            path = '/xs0/seed/'+pathData.network+'_'+pathData.station+'/'+str(pathData.date.year)+'/'+str(pathData.date.year)+'_'+ str('{0:0=3d}'.format(julianday))+'_'+pathData.network+'_'+pathData.station+'/'
        
        #Build the path for the input file
        dataInPath = ''
        if len(glob.glob(path+pathData.channel+'.*')) > 0:
            dataInPath = str(glob.glob(path+pathData.channel+'.*')[0])
        elif len(pathData.location) > 0 and len(glob.glob(path+pathData.channel[0]+"C"+pathData.location[0]+'.*')) > 0:
            dataInPath = str(glob.glob(path+pathData.channel[0]+"C"+pathData.location[0]+'.*')[0])
        else:
            logging.warn('Unable to find input file ' + str(path+pathData.channel+'.*'))
    
        #Build the path for the output file
        patternBH = re.compile("^BC*")
        outChannels = []
        if(pathData.channel == 'BC8'):
            outChannels = ['BHZ'] 
        elif(patternBH.match(pathData.channel)):
            outChannels = ['BHZ', 'BH1', 'BH2']                          
        else:
            outChannels = ['LHZ'] 
        dataOutPath = ''
        
        for outChannel in outChannels:
            dataOutPath = path + pathData.location + "_" + outChannel + ".512.seed"
            print("OUT PATH = " + dataOutPath)
            print("IN PATH = " + dataInPath)
            #Compute sine cal for the given data path
            if(os.path.isfile(dataInPath) and os.path.isfile(dataOutPath)):
                pc = ComputeCalibrations.ComputeCalibrations(dataInPath, dataOutPath, pathData.date, str('{0:0=3d}'.format(julianday)), pathData.cal_duration, pathData.cal_id, outChannel, pathData.network, pathData.station, pathData.location, dbconn)
                if(calType == 'sine'):
                    pc.computeSineCal() 
                elif(calType == 'step'):
                    pc.computeStepCal()
                    
                    
#main program here
if __name__ == "__main__":
    #Global Variables 
    global calType    
    
    #Setup logging
    logging.basicConfig(filename='logs/error.log', level=logging.INFO)
    
    #Read data from config file
    config = ParseConfig.ParseConfig()
    #Connect to the database
    dbconn = connectToDatabase(config)
    
    #Create a list of all calibration types that were entered in the config file
    calTypes = config.calibrationType.split(',')

    for ct in calTypes:
        calType = ct
        #Query database to get path data for sine calibrations
        #pool = Pool(10)
        pathData = getPathData()
        for path in pathData:
            computeNewCal(path)
        #pool.map(computeNewCal, pathData)
