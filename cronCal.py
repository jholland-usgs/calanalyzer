import datalesstools
import detect_cal
import glob

from obspy.core import UTCDateTime

today = UTCDateTime.now()
networks = ['CU','GS','IC','IU','IW','NE','US','XX']
networks = ['CU','GS','IC','IU','IW','NE','US']
output = ''
debug = False
detect_cal_filepath = '/home/ambaker/calanalyzer/detect_cal.py'

if debug:
    print 'Scan started at %s' % today.strftime('%Y,%j %H:%M:%S')

for network in networks:
    dataless = datalesstools.getDataless(network)
    
    #check 1d, 7d, 30d, 60d, 90d, 180d ago
    days_ago = [1,7,30,60,90,180]
    for day in days_ago:
        if debug:
            print 'python %s -n %s -b %s -e %s' % (detect_cal_filepath, network, (today - day * 86400).strftime('%Y,%j'), (today - day * 86400).strftime('%Y,%j'))
        output += detect_cal.find_files(network, '*', (today - day * 86400), (today - day * 86400), dataless)

    #check one month last year
    months = range(1, 13)
    months.append(1)
    index = today.julday % 12
    begindate = UTCDateTime('%s-%s-01' % (today.year - 1, months[index]))
    enddate = UTCDateTime('%s-%s-01' % (today.year - 1, months[index + 1]))
    if debug:
        print 'python %s -n %s -b %s -e %s' % (detect_cal_filepath, network, begindate.strftime('%Y,%j'), enddate.strftime('%Y,%j'))
    output += detect_cal.find_files(network, '*', begindate, enddate, dataless)
        

    #check an entire historical year
    years = range(1972, today.year)
    index = today.julday % len(years)
    begindate = UTCDateTime('%s-01-01' % years[index])
    enddate = UTCDateTime('%s-12-31' % years[index])
    if debug:
        print 'python %s -n %s -b %s -e %s' % (detect_cal_filepath, network, begindate.strftime('%Y,%j'), enddate.strftime('%Y,%j'))
    output += detect_cal.find_files(network, '*', begindate, enddate, dataless)

if debug:
    output = output.split('\n')
    output_temp = []
    for line in output:
        if line != '':
            output_temp.append(line)
    print '\n'.join(output_temp)

    print '%.2f seconds elapsed' % (UTCDateTime.now() - today)