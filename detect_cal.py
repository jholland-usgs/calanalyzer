import argparse
import caluser
import commands
import database
import datalesstools
import glob
import os
import struct

from obspy.core import UTCDateTime

debug = True

def get_arguments():
    'Parses the command line arguments'
    parser = argparse.ArgumentParser(description='Code to compare data availability')

    #sets flag for the network
    parser.add_argument('-n', action = "store",dest="net", default = "NN", help="       Network to check: NN", type = str, required = True)

    #sets flag for the station (optional)
    parser.add_argument('-s', action = "store",dest="sta", default = "*", help="       Station to check: SSSS", type = str, required = False)

    #sets flag for the begin date
    parser.add_argument('-b', action = "store",dest="bdate", default = "2015,001", help="Date to check: YYYY,JJJ or YYYY-MM-DD", type = str, required = True)

    #sets flag for the end date
    parser.add_argument('-e', action = "store",dest="edate", default = None, help="Date to check: YYYY,JJJ or YYYY-MM-DD", type = str, required = False)

    parserval = parser.parse_args()
    return parserval

def set_arguments():
    'Sets the arguments'
    args = get_arguments()
    net = args.net
    sta = args.sta
    try:
        bdate = UTCDateTime(args.bdate)
    except:
        print 'Invalid begin date format (YYY,JJJ or YYYY-MM-DD)'
    try:
        if args.edate:
            edate = UTCDateTime(args.edate)
        else:
            edate = UTCDateTime.now()
    except:
        print 'Invalid end date format (YYY,JJJ or YYYY-MM-DD)'
    assert bdate <= edate, 'Begin date is greater than end date'
    assert edate <= UTCDateTime.now(), 'End date is in the future'
    return net, sta, bdate, edate

def find_files(net, sta, bdate, edate, dataless=None):
    'Find the files that may contain calibrations'
    date = bdate
    output = []
    if dataless == None:
        dataless = datalesstools.getDataless(net + sta)
    while date <= edate:
        filepath = '/msd/%s_%s/%s/%s/*[BL]HZ.512.seed' % (net, sta, date.strftime('%Y'), date.strftime('%j'))
        filepaths = glob.glob(filepath)
        cals = find_calibrations(filepaths)
        output.append(add_calibrations(dataless, cals))
        date += 86400
    return '\n'.join(output)

def find_calibrations(filepaths):
    'Attempts to retrieve calibrations by looking for calibration blockettes (300, 310, 320)'
    #mostly written by Adam Ringler
    calibrations = []
    for filepath in filepaths:
        _,_,net_sta,year,jday,loc_chan_reclen_seed = filepath.split('/')
        date = UTCDateTime(year + jday)
        net, sta = net_sta.split('_')
        loc,chan = loc_chan_reclen_seed.split('.')[0].split('_')
        #read the first file and get the record length from blockette 1000
        fh = open(filepath, 'rb')
        record = fh.read(256)
        index = struct.unpack('>H', record[46:48])[0]
        file_stats = os.stat(filepath)
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
                            calibrations.append({'net': net, 'sta': sta, 'loc': loc, 'chan': chan, 'date': date, 'type': 300, 'startdate': UTCDateTime(stime), 'flags': calFlags, 'num_step_cals': numStepCals, 'step_duration': duration, 'interval_duration': intervalDuration, 'amplitude': amplitude, 'channel': calInput})
                        if blockette_type == 310:
                            #blockette for sine cals
                            signalPeriod,amplitude,calInput = struct.unpack('>ff3s',record[index+20:index+31])
                            calibrations.append({'net': net, 'sta': sta, 'loc': loc, 'chan': chan, 'date': date, 'type': 310, 'startdate': UTCDateTime(stime), 'flags': calFlags, 'cal_duration': duration, 'signal_period': signalPeriod, 'amplitude': amplitude, 'channel': calInput})
                        if blockette_type == 320:
                            #blockette for psuedorandom cals
                            amplitude,calInput = struct.unpack('>f3s', record[index+20:index+27])
                            calibrations.append({'net': net, 'sta': sta, 'loc': loc, 'chan': chan, 'date': date, 'type': 320, 'startdate': UTCDateTime(stime), 'flags': calFlags, 'cal_duration': duration, 'ptp_amplitude': amplitude, 'channel': calInput})
                        if blockette_type == 390:
                            #blockette for generic cals, currently unused
                            amplitude,calInput = struct.unpack('>f3s', record[index+20:index+27])
                            calibrations.append({'net': net, 'sta': sta, 'loc': loc, 'chan': chan, 'date': date, 'type': 390, 'startdate': UTCDateTime(stime), 'flags': calFlags, 'duration': duration, 'amplitude': amplitude, 'channel': calInput})
                            if debug:
                                print 'Generic cal:', net, sta, cal['startdate']
        except:
            pass
        fh.close()
    return calibrations

def add_calibrations(dataless, cals):
    'Adds the calibrations to the database if not a duplicate'
    dbname, username, host, password = caluser.info()
    caldb = database.Database(dbname, username, host, password)
    output = []
    for cal in cals:
        try:
            network_id = get_network_id(caldb, cal)
            station_id = get_station_id(caldb, cal, network_id)
            location_id = get_location_id(caldb, cal, station_id)
            sensor_id = get_sensor_id(caldb, cal, location_id, dataless)
            #in rare cases there will be calibration data in a seedfile...
            #without it being described in metadata (i.e. IU_KOWA 10 for 2017,130)
            if sensor_id == None:
                cals.pop(cals.index(cal))
                break
            #check if calibration already exists in database
            query = """SELECT tbl_%s.pk_id, tbl_networks.network, tbl_stations.station_name, tbl_locations.location, tbl_sensors.sensor, tbl_%s.startdate, tbl_%s.channel, tbl_%s.%s FROM tbl_networks
                JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
                JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
                JOIN tbl_sensors ON tbl_sensors.fk_locationid = tbl_locations.pk_id
                JOIN tbl_%s ON tbl_%s.fk_sensorid = tbl_sensors.pk_id
                WHERE network = '%s' AND station_name = '%s' AND tbl_%s.startdate = '%s' AND location = '%s' AND channel = '%s' AND %s = %s"""
            if cal['type'] == 300:
                query %= (cal['type'], cal['type'], cal['type'], cal['type'], 'step_duration', cal['type'], cal['type'], cal['net'], cal['sta'], cal['type'], cal['startdate'], cal['loc'], cal['channel'], 'step_duration', cal['step_duration'])
            else:
                query %= (cal['type'], cal['type'], cal['type'], cal['type'], 'cal_duration', cal['type'], cal['type'], cal['net'], cal['sta'], cal['type'], cal['startdate'], cal['loc'], cal['channel'], 'cal_duration', cal['cal_duration'])
            if not caldb.select_query(query):
                #cal does not exist in the database, insert it
                if cal['type'] == 300:
                    query = """INSERT INTO tbl_%s (fk_sensorid, type, startdate, flags, num_step_cals, step_duration, interval_duration, amplitude, channel) VALUES (%s, '%s', '%s', '%s', %s, %s, %s, %s, '%s')""" % (cal['type'], sensor_id, cal['type'], cal['startdate'], cal['flags'], cal['num_step_cals'], cal['step_duration'], cal['interval_duration'], cal['amplitude'], cal['channel'])
                if cal['type'] == 310:
                    query = """INSERT INTO tbl_%s (fk_sensorid, type, startdate, flags, cal_duration, signal_period, amplitude, channel) VALUES (%s, '%s', '%s', '%s', %s, %s, %s, '%s')""" % (cal['type'], sensor_id, cal['type'], cal['startdate'], cal['flags'], cal['cal_duration'], cal['signal_period'], cal['amplitude'], cal['channel'])
                if cal['type'] == 320:
                    query = """INSERT INTO tbl_%s (fk_sensorid, type, startdate, flags, cal_duration, ptp_amplitude, channel) VALUES (%s, '%s', '%s', '%s', %s, %s, '%s')""" % (cal['type'], sensor_id, cal['type'], cal['startdate'], cal['flags'], cal['cal_duration'], cal['ptp_amplitude'], cal['channel'])
                caldb.insert_query(query)
                if debug:
                    print '+Calibration inserted: %2s_%-5s %2s-%3s %s' % (cal['net'], cal['sta'], cal['loc'], cal['chan'], cal['startdate'].strftime('%Y,%j %H:%M:%S'))
                else:
                    output.append('+Calibration inserted: %2s_%-5s %2s-%3s %s' % (cal['net'], cal['sta'], cal['loc'], cal['chan'], cal['startdate'].strftime('%Y,%j %H:%M:%S')))
            else:
                if debug:
                    print ' Calibration detected: %2s_%-5s %2s-%3s %s' % (cal['net'], cal['sta'], cal['loc'], cal['chan'], cal['startdate'].strftime('%Y,%j %H:%M:%S'))
                else:
                    output.append(' Calibration detected: %2s_%-5s %2s-%3s %s' % (cal['net'], cal['sta'], cal['loc'], cal['chan'], cal['startdate'].strftime('%Y,%j %H:%M:%S')))
        except Exception, e:
            print 'Error for %s_%s %s-%s %s:' % (cal['net'], cal['sta'], cal['loc'], cal['chan'], cal['startdate'].strftime('%Y,%j %H:%M:%S')), e 
            output.append('Error for %s_%s %s-%s %s: %s' % (cal['net'], cal['sta'], cal['loc'], cal['chan'], cal['startdate'].strftime('%Y,%j %H:%M:%S'), e))
    caldb.close_connection()
    return '\n'.join(output)

def get_network_id(db, cal):
    'Returns the primary key of the network'
    #return the key if network exists in the database
    query = """SELECT pk_id
               FROM tbl_networks
               WHERE network = '%s'""" % (cal['net'])
    network_id = db.select_query(query, 1)
    if network_id == None:
        #network not found in database
        query = """INSERT INTO tbl_networks (network)
                   VALUES ('%s')
                   RETURNING pk_id""" % (cal['net'])
        network_id = db.insert_query(query, True)
    return network_id[0]

def get_station_id(db, cal, network_id):
    'Returns the primary key of the station'
    #return the key if station exists in the database
    query = """SELECT tbl_stations.pk_id FROM tbl_networks
               JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
               WHERE network = '%s' AND station_name = '%s'""" % (cal['net'], cal['sta'])
    station_id = db.select_query(query, 1)
    #station not found in database
    if station_id == None:
        query = """INSERT INTO tbl_stations (fk_networkid, station_name)
                   VALUES (%s, '%s') RETURNING pk_id""" % (network_id, cal['sta'])
        station_id = db.insert_query(query, True)
    return station_id[0]

def get_location_id(db, cal, station_id):
    'Returns the primary key of the location'
    #return the key if location exists in the database
    query = """SELECT tbl_locations.pk_id FROM tbl_networks
               JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
               JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
               WHERE network = '%s' AND station_name = '%s' AND location = '%s' """ % (cal['net'], cal['sta'], cal['loc'])
    location_id = db.select_query(query, 1)
    #location not found in database
    if location_id == None:
        query = """INSERT INTO tbl_locations (fk_stationid, location)
                    VALUES (%s, '%s') RETURNING pk_id""" % (station_id, cal['loc'])
        location_id = db.insert_query(query, True)
    return location_id[0]

def get_sensor_id(db, cal, location_id, dataless):
    'Returns the primary key of the sensor'
    for station in dataless.stations:
        if station[0].blockette_type == 50 and station[0].station_call_letters == cal['sta']:
            for blockette in station:
                if blockette.blockette_type == 52 and blockette.location_identifier == cal['loc'] and blockette.start_date <= cal['date'] <= blockette.end_date:
                    query = """SELECT tbl_sensors.pk_id FROM tbl_networks
                               JOIN tbl_stations ON tbl_stations.fk_networkid = tbl_networks.pk_id
                               JOIN tbl_locations ON tbl_locations.fk_stationid = tbl_stations.pk_id
                               JOIN tbl_sensors ON tbl_sensors.fk_locationid = tbl_locations.pk_id
                               WHERE network = '%s' AND station_name = '%s' AND location = '%s' AND startdate <= '%s' AND enddate >= '%s'""" % (cal['net'], cal['sta'], cal['loc'], blockette.start_date, blockette.end_date)
                    sensor_id = db.select_query(query, 1)
                    if not sensor_id:
                        fp = '/msd/%s_%s/%s/%s/%s-%s.512.seed' % (cal['net'], cal['sta'], cal['startdate'].strftime('%Y'), cal['startdate'].strftime('%j'), cal['loc'], cal['chan'])
                        dict_b031, dict_b033, dict_b034 = getDictionaries(fp, cal['net'])
                        sensor_name = fetchInstrument(dict_b033, blockette.instrument_identifier)
                        query = """INSERT INTO tbl_sensors (fk_locationid, sensor, startdate, enddate)
                                    VALUES (%s, '%s', '%s', '%s') RETURNING pk_id""" % (location_id, sensor_name, blockette.start_date, blockette.end_date)
                        sensor_id = db.insert_query(query, 1)
                    return sensor_id[0]
    return None

def getDictionaries(filepath, net):
    command = '/home/ambaker/apps/rdseed -f %s -g ~/apps/%s.dataless -a' % (filepath, net)
    b031, b033, b034 = parseRDSEEDAbbreviations(commands.getstatusoutput(command)[-1])
    return b031, b033, b034

def parseRDSEEDAbbreviations(output):
    b031 = []
    b033 = []
    b034 = []
    for group in output.split('#\t\t\n'):
        if 'B031' == group[:4]:
        #     dictionary = {}
        #     for line in group.strip().split('\n'):
        #         if 'B031F03' in line:
        #             dictionary['comment code id'] = int(line.split('  ')[-1].strip())
        #         elif 'B031F04' in line:
        #             dictionary['comment class code'] = line.split('  ')[-1].strip()
        #         elif 'B031F05' in line:
        #             dictionary['comment text'] = line.split('  ')[-1].strip()
        #         elif 'B031F06' in line:
        #             dictionary['comment units'] = line.split('  ')[-1].strip()
        #     b031.append(dictionary)
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
        #     dictionary = {}
        #     for line in group.strip().split('\n'):
        #         if 'B034F03' in line:
        #             dictionary['unit code'] = int(line.split('  ')[-1].strip())
        #         elif 'B034F04' in line:
        #             dictionary['unit name'] = line.split('  ')[-1].strip()
        #         elif 'B034F05' in line:
        #             dictionary['unit description'] = line.split('  ')[-1].strip()
        #     b034.append(dictionary)
    return b031, b033, b034

# def fetchComment(dictB031, value):
#     'Blockette 31, used for describing comments'
#     for comment in dictB031:
#         if value == comment['comment code id']:
#             return [comment['comment text'], comment['comment units'], comment['comment class code']]
#     return ['No comments found', 'N/A', '0']

def fetchInstrument(dict_b033, value):
    'Blockette 33, used for describing instruments'
    for instrument in dict_b033:
        if value == instrument['description key code']:
            return instrument['abbreviation description']
    return 'No instrument found'

# def fetchUnit(dictB034, value):
#     'Blockette 34, used for describing units'
#     for unit in dictB034:
#         if value == unit['unit code']:
#             return [unit['unit name'], unit['unit description']]
#     return ['None', 'No units found']

if __name__ == '__main__':
    net, sta, bdate, edate = set_arguments()
    find_files(net, sta, bdate, edate)