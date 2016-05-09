#!/usr/bin/python
'''
Created on Sep 18, 2015

@author: Nick Falco
'''
'''
'    Connect to the database using the db info read from the config file
'''

import datetime
import glob
import logging
from multiprocessing import Pool
import os
import re

from obspy.io import xseed
import obspy
from obspy.core import UTCDateTime
import psycopg2

from src import ComputeCalibrations
from src import ParseConfig
from src import PathData


def connectToDatabase(config):
    try:
        conn = psycopg2.connect("dbname='" + config.dbname +
                                "' user='" + config.username +
                                "' host='" + config.host +
                                "' password='" + config.password +
                                "'")
    except:
        logging('I am unable to connect to the database')
    return conn

''' 
'    Returns the set of all networks/stations and calibration
'    dates for a given calibration type
'''
def getPathData():
    # Determine the correct table to query based on the calibration type
    durationType = ''
    if(calType == 'sine'):
        calTable = 'tbl_310'
        durationType = 'cal_duration'
    elif(calType == "random"):
        calTable = 'tbl_320'
        durationType = 'cal_duration'
    elif(calType == "step"):
        calTable = 'tbl_300'
        durationType = 'step_duration'
    # Connect to the database
    dbconn = connectToDatabase(config)
    # Create cursor used to query the database
    cur = dbconn.cursor()
    # Query the database to retrieve all records for the
    # given calibration type
    query = "SELECT DISTINCT " + calTable + """.pk_id, tbl_networks.network,
            tbl_stations.station_name, tbl_locations.location, """ + \
            calTable + ".startdate, " + calTable + ".channel, " + \
            calTable + "." + durationType + (', ' + calTable + '.signal_period' if calType == 'sine' else '') + \
            """
             FROM tbl_networks JOIN tbl_stations ON tbl_networks.pk_id = tbl_stations.fk_networkid
                               JOIN tbl_locations ON tbl_stations.pk_id = tbl_locations.fk_stationid
                               JOIN tbl_sensors ON tbl_locations.pk_id = tbl_sensors.fk_locationid
                               JOIN """ + calTable + " ON tbl_sensors.pk_id = " + calTable + """.fk_sensorid
                               LEFT JOIN """ + calTable + "calresults ON " + calTable + ".pk_id = " + calTable + "calresults.fk_calibrationid " + \
            "ORDER BY tbl_networks.network DESC, startdate DESC"
    print query
    cur.execute(query)
    rows = cur.fetchall()   
    cur.close()
    dbconn.close()

    pathlist = []
    for row in rows:
        if calType == 'sine':
            sig_period=row[7]
        else:
            sig_period=''
        pathlist.append(PathData.PathData(cal_id=row[0], network=row[1], station=row[
                        2], location=row[3], date=row[4], channel=row[5], cal_duration=row[6], signal_period=sig_period, ps=None))
    return pathlist

'''
'    Computes the specified calibration using the information provided by the database
'''
def computeNewCal(pathData):
    # Calculate equivalent julian calendar day
    julianday = UTCDateTime(
        pathData.date.year, pathData.date.month, pathData.date.day, 0, 0).julday
    # Build the relative data path
    if(pathData.network == 'US'):
        path = '/xs1/seed/' + pathData.network + '_' + pathData.station + '/' + str(pathData.date.year) + '/' + str(
            pathData.date.year) + '_' + str('{0:0=3d}'.format(julianday)) + '_' + pathData.network + '_' + pathData.station + '/'
    else:
        path = '/xs0/seed/' + pathData.network + '_' + pathData.station + '/' + str(pathData.date.year) + '/' + str(
            pathData.date.year) + '_' + str('{0:0=3d}'.format(julianday)) + '_' + pathData.network + '_' + pathData.station + '/'

    # Build the path for the input file
    dataInPath = ''
    if len(glob.glob(path + pathData.channel + '.*')) > 0 and pathData.channel != '':
        dataInPath = str(glob.glob(path + pathData.channel + '.*')[0])
    elif pathData.location > 0 and len(glob.glob(path + pathData.channel[0] + "C" + 
                            pathData.location[0] + '.*')) > 0 and pathData.channel != '':
        dataInPath = str(
            glob.glob(path + pathData.channel[0] + "C" + pathData.location[0] + '.*')[0])
    elif pathData.channel == '':
        pathData.channel = 'BH' + pathData.location[0]
    else:
        logging.warn(
            'Unable to find input file ' + str(path + pathData.channel + '.*'))
    # Build the path for the output file
    patternBH = re.compile("^BC*")
    patternHH = re.compile("^HC*")
    outChannels = []
    if(pathData.channel == 'BC8'):
        outChannels = ['BHZ']
    elif(patternBH.match(pathData.channel)):
        outChannels = ['BHZ', 'BH1', 'BH2']
    elif(patternHH.match(pathData.channel)):
        outChannels = ['HHZ', 'HH1', 'HH2']
    else:
        outChannels = ['LHZ']
    dataOutPath = ''
    for outChannel in outChannels:
        dataOutPath = path + pathData.location + "_" + outChannel + ".512.seed"
        # Compute sine cal for the given data path
        if(os.path.isfile(dataInPath) and os.path.isfile(dataOutPath)):
            # Connect to the database
            dbconn = connectToDatabase(config)
            pc = ComputeCalibrations.ComputeCalibrations(dataInPath, dataOutPath, pathData.date, str('{0:0=3d}'.format(julianday)),
                                                         pathData.cal_duration, pathData.cal_id, pathData.network, pathData.station,
                                                         pathData.location, outChannel, pathData.signal_period, dbconn, pathData.ps)
            if(calType == 'sine'):
                pc.computeSineCal()
            elif(calType == 'step'):
                pc.computeStepCal()
            elif(calType == 'random'):
                pc.computeRandomCal()
            else:
                print 'unknown calibration type'
            dbconn.close()
        else:
            print 'path doesnt exists - ' + str(dataOutPath)
'''
'    If specific calibration information is provided  use the provided
'    information rather getting the information from the file system
'''
def computeNewCalManualOverride():
    if not os.path.exists(config.outputloc):
        print 'output file path - ' + config.outputloc + ' does not exist'
    elif not os.path.exists(config.inputloc):
        print 'input file path - ' + config.inputloc + ' does not exist'
    else:
        # Calculate equivalent julian calendar day
        stats = obspy.read(config.outputloc)[0].stats
        date = datetime.datetime.strptime(
            config.startdate, '%Y-%m-%d %H:%M:%S')
        julianday = UTCDateTime(date.year, date.month, date.day, 0, 0).julday
        pc = ComputeCalibrations.ComputeCalibrations(config.inputloc, config.outputloc,
                                                     date, str(
                                                         '{0:0=3d}'.format(julianday)),
                                                     float(config.duration), None, stats[
                                                         'network'], stats['station'],
                                                     stats['location'], stats[
                                                         'channel'],
                                                     None, None, config.sentype)
        if(calType == 'sine'):
            pc.computeSineCal()
        elif(calType == 'step'):
            pc.computeStepCal()
        elif(calType == 'random'):
            pc.computeRandomCal()

# main program here
if __name__ == "__main__":
    # Global Variables
    global calType
    global config
    global debug 
    
    debug = True
    
    # make a logs directory if it doesn't already exist
    if not os.path.isdir('logs'):
        os.mkdir('logs')

    # Setup logging
    logging.basicConfig(filename='logs/error.log', level=logging.INFO)

    # Read data from config file
    config = ParseConfig.ParseConfig()

    # Create a list of all calibration types that were entered in the config
    # file
    calTypes = config.calibrationType.split(',')
    # remove temp directory and contents if it already exists
    os.system('rm -rf temp')

    # make a directory called temp to store image files to be written to the
    # database
    os.mkdir('temp')
    for ct in calTypes:
        calType = ct
        if debug: # not multithreaded for debugging.
            if(config.sentype is None and config.startdate is None and int(config.duration) == 0 and
               config.inputloc is None and config.outputloc is None):
                pathData = getPathData()
                calculatedNetwork = ''
                for path in pathData:
                    if calType == 'sine':
                        if path.network != calculatedNetwork:
                            ps = xseed.Parser('/APPS/metadata/SEED/'+path.network+'.dataless')
                            calculatedNetwork = path.network
                        path.ps = ps
                    else:
                        path.ps = None
                    computeNewCal(path)
            elif((config.sentype is not None) and (config.startdate is not None) and
                 (config.duration != '0') and (config.inputloc is not None) and
                 (config.outputloc is not None)):
                computeNewCalManualOverride()
            else:
                print(
                    "There was a problem with way the program was called. Please verify your input.")
        else:
            # Query database to get path data for sine calibrations
            pool = Pool(10)
            # If specific calibration information is provided  use the provided
            # information rather than querying the database
            if(config.sentype is None and config.startdate is None and int(config.duration) == 0 and
               config.inputloc is None and config.outputloc is None):
                pathData = getPathData()
                calculatedNetwork = ''
                for path in pathData:
                    if path.network != calculatedNetwork:
                        ps = xseed.Parser('/APPS/metadata/SEED/'+path.network+'.dataless')
                        calculatedNetwork = path.network
                    path.ps = ps
                pool.map(computeNewCal, pathData)
            elif((config.sentype != None) and (config.startdate != None) and (config.duration != None) and (config.inputloc != None) and (config.outputloc != None)):
                computeNewCalManualOverride()
            else:
                print("There was a problem with way the program was called. Please verify your input.")
            

    #os.system('rm -rf temp')
    exit(1)
