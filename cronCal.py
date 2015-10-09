#!/usr/bin/env python

import commands
import glob
import multical
from obspy.core import UTCDateTime

jday = UTCDateTime.now().julday
yearCur = UTCDateTime.now().year

#get first year
years = []
paths = glob.glob('/xs[01]/seed/*/*')
for path in paths:
	if path.split('/')[-1] not in years:
		if path.split('/')[-1].isdigit():
			years.append(path.split('/')[-1])
years.sort()
yearFirst = years[0]

#first, check one year
# yearOne = years[jday % (len(years) - 1)]
# output = commands.getstatusoutput('python multical.py -b ' + str(yearOne) + ',001 -e ' + str(yearOne) + ',366')
# print output

#second, check one month last year
# month = jday % 15 * 25 + 1
# output = commands.getstatusoutput('python multical.py -b ' + str(yearCur - 1) + ',' + str(month).zfill(3) + ' -e ' + str(yearCur - 1) + ',' + str(month + 24).zfill(3))
# print output

#third, check in steps
steps = [7, 30, 60, 90, 180]
for step in steps:
	if jday > steps:
		output = commands.getstatusoutput('python multical.py -b ' + str(yearCur) + ',' + str(step).zfill(3) + ' -e ' + str(yearCur) + ',' + str(step).zfill(3))
		print output