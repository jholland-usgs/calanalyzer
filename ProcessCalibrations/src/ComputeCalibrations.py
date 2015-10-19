'''
Created on Sep 18, 2015

@author: nfalco
'''

from obspy.core import read, UTCDateTime, Stream
import numpy as np
import math
import cmath as CM
import sys
import commands
import matplotlib.pyplot as plt
from obspy.signal import cornFreq2Paz
from scipy.optimize import fmin
import numpy
from _mysql import NULL

class ComputeCalibrations(object):
    
    def __init__(self, dataInLoc, dataOutLoc, startdate, julianday, cal_duration, cal_id, outChannel, network, station, location, dbconn):
        self.dataInLoc = dataInLoc #Location where the input data is located 
        self.dataOutLoc = dataOutLoc #Location where the output seed data is located
        self.startdate = startdate #Start data for the calibration
        self.julianday = julianday
        self.cal_duration = cal_duration #Duration of the calibration in milliseconds 
        self.cal_id = cal_id #Database primary key of the calibration
        self.outChannel = outChannel #Channel for which the calibration was calculated (eg. BHZ, BH1, BH2, etc.)
        self.network = network
        self.station = station
        self.location = location
        self.dbconn = dbconn #Database connection object

    def computeSineCal(self):
        try:
            # Read in BH and BC
            dataIN = read(self.dataInLoc)
            dataOUT = read(self.dataOutLoc)
            #print("Calibration Duration = " + str(self.cal_duration))
            # Trim data to start and end time
            stime = UTCDateTime(str(self.startdate))
            #print ("Data In Length Before Trim = " + str(len(dataIN)))
            dataIN.trim(starttime=stime, endtime=stime + self.cal_duration)
            #print ("Data In Length After Trim = " + str(len(dataIN)))
            dataOUT.trim(starttime=stime, endtime=stime + self.cal_duration)
        
            # Calculate RMS of both traces and divide
            dataINRMS = math.sqrt(2.)*sum(np.square(dataIN[0].data))
            dataINRMS /= float(dataIN[0].stats.npts)
            dataOUTRMS = math.sqrt(2.)*sum(np.square(dataOUT[0].data))
            dataINRMS /= float(dataOUT[0].stats.npts)  
        
            #Write results to datbase      
            cur = self.dbconn.cursor()
            cur.execute("""INSERT INTO tbl_310calresults (fk_calibrationid, input_rms, output_rms, outchannel, coil_constant)
                           VALUES ("""+ "'" + str(self.cal_id) + "', '" + str(dataINRMS) + "', '" + str(dataOUTRMS) + "', '" + str(self.outChannel) + "', '" + str(dataINRMS/dataOUTRMS) + "')")
           
            self.dbconn.commit()
        except:
            print("Unexpected error:", sys.exc_info()[0])
            
    def computeStepCal(self):
        
        year = self.startdate.year
        day = self.julianday
        sta = self.station
        chan = self.outChannel
        loc = self.location
        duration = self.cal_duration / 10000.0
        
        mdgetstr = '/home/aringler/data_stuff/checkstep/./mdget.py -n IU -l ' + str(loc) + ' -c ' + str(chan) + \
            ' -s ' + str(sta) + ' -t ' + str(year) + '-' + str(day) + ' -o' + \
            ' \'instrument type\''

        output = commands.getstatusoutput(mdgetstr)

        # These might not be consistent names for sensors between networks
        try:
            output = output[1].split(',')[4]
            print ("output sensor = " + output)
        except:
            print ('We have a problem with: ' + str(sta) + ' ' + str(chan) + ' ' + str(loc))
        sensor = ''
        if 'E300' in output:
            sensor = 'STS1'
        elif 'Trillium' in output:
            sensor='T240'
        elif 'STS-1' in output:
            sensor = 'STS1'
        elif 'STS-2' in output:
            sensor = 'STS2'
        pz = self.pzvals(sensor)
        
        try:
            stOUT = Stream()   
            stime = UTCDateTime(self.startdate) - 5*60  
            stOUT = read(self.dataOutLoc,starttime = stime, endtime = stime + duration + 5*60 +900 )
            stOUT.merge()
            stIN = read(self.dataInLoc,starttime = stime, endtime = stime + duration + 5*60  + 900)
            stIN.merge()
            trIN = stIN[0]
            trOUT = stOUT[0]
            trOUT.filter('lowpass',freq=.1)
            trIN.filter('lowpass',freq=.1)
            trIN.detrend('constant')
            trIN.normalize()
            trOUT.detrend('constant')
            trOUT.normalize()
            temp=trOUT.copy()
            temp.trim(endtime = stime + int(duration/2.))
            if temp.max() < 0.0:
                trOUT.data = - trOUT.data
        except:
            print ('Problem with : ' + str(sta) + ' ' + str(chan) + ' ' + str(loc))
        
        try:
            f = 1. /(2*math.pi / abs(pz['poles'][0]))
            #f = 2. * math.pi / 360.
            h = abs(pz['poles'][0].real)/abs(pz['poles'][0])
            sen = 10.0            
            print ('trIN = ' + str(trIN) + "\ntrOUT = " + str(trOUT))
            print ('Using: h=' + str(h) + ' f=' + str(f) + ' sen = ' + str(sen))
            x = numpy.array([f, h, sen])
            try:
                bf = fmin(self.resi,x, args=(trIN, trOUT),xtol=10**-8,ftol=10**-3,disp=False)
            except:
                bf = x 
            #bf = x    
            print ('Here is the best fit: ' + str(bf))
        except:
            print ('Unable to calculate ' + str(sta) + ' ' + str(chan) + ' ' + str(loc))
    
        try:
            pazNOM = cornFreq2Paz(f,h)
            pazNOM['zeros']=[0.+0.j]
    
            pazPERT = cornFreq2Paz(bf[0],bf[1])
            pazPERT['zeros']=[0]
    
            trOUTsimPert = trOUT.copy()
            trOUTsimPert.simulate(paz_remove = pazPERT)
            trOUTsimPert.trim(trOUTsimPert.stats.starttime + 50,trOUTsimPert.stats.endtime - 50)
            trOUTsimPert.detrend('constant')
            trOUTsimPert.normalize()
    
            trOUTsim = trOUT.copy()
    
            trOUTsim.simulate(paz_remove = pazNOM)
            trOUTsim.trim(trOUTsim.stats.starttime + 50,trOUTsim.stats.endtime - 50)
            trOUTsim.detrend('constant')
            trOUTsim.normalize()
    
    
            trIN.trim(trIN.stats.starttime + 50,trIN.stats.endtime - 50)
            trIN.detrend('constant')
            trIN.normalize()
        
            compOUT = sum((trOUTsim.data - trIN.data)**2)
            compOUTPERT = sum((trOUTsimPert.data - trIN.data)**2)
        except:
            print ('Unable to do calculation on ' + sta + ' ' + loc + ' ' + chan)
    
        try:
            plt.clf()
            t = numpy.arange(0,trOUTsim.stats.npts /trOUTsim.stats.sampling_rate,trOUTsim.stats.delta)
            plt.plot(t,trIN.data,'b',label = 'Input')
            plt.plot(t,trOUTsim.data,'k',label='h=' + str(round(h,6)) + ' f=' + str(round(f,6)) + ' resi=' + str(round(compOUT,6)))
            plt.plot(t,trOUTsimPert.data,'g',label = 'h=' + str(round(bf[1],6)) + ' f=' + str(round(bf[0],6))+ ' resi=' + str(round(compOUTPERT,6)))
            plt.xlabel('Time (s)')
            plt.ylabel('Cnts normalized')
            plt.title('Step Calibration ' + trOUT.stats.station + ' ' + str(trOUT.stats.starttime.year) + ' ' + str(trOUT.stats.starttime.julday).zfill(3))
            plt.legend()
            plt.xlim((0, 2500))
            plt.ylim((-1.5, 1.5))
            plt.show()
            plt.savefig(str(trOUT.stats.station) + str(chan) + str(loc) + str(year) + str(day) + 'step.jpg',format = "jpeg", dpi = 400)
        except:
            print ('Unable to plot: ' + sta + ' ' + loc + ' ' + chan)
        
        #insert results into the database
        query = '''INSERT INTO tbl_300calresults (fk_calibrationid, nominal_cornerfreq, nominal_dampingratio, nominal_resi, fitted_cornerfreq, fitted_dampingratio, fitted_resi, stepcal_img)
                VALUES(''' + str(self.cal_id) + ',' + str(round(f,6))+',' + str(round(h,6)) + ',' + str(round(compOUT,6)) + ',' + str(round(bf[0],6)) + ',' + str(round(bf[1],6)) + ',' +  str(round(compOUTPERT,6)) + ",'" + (str(trOUT.stats.station) + str(chan) + str(loc) + str(year) + str(day) + 'step.jpg') + "')"
        cur = self.dbconn.cursor()
        cur.execute(query)
        self.dbconn.commit()

    def pzvals(self, sensor):
        if sensor == 'STS2':
            pz ={'zeros': [0.], 'poles': [-0.035647 - 0.036879j, \
                -0.035647 + 0.036879j], 'gain': 1., 'sensitivity': 1. }
        elif sensor == 'STS1':
            pz ={'zeros': [0.], 'poles': [-0.01234 - 0.01234j, \
                -0.01234 + 0.01234j], 'gain': 1., 'sensitivity': 1. }
        elif sensor == 'T240':
            pz ={'zeros': [0.], 'poles': [-0.0178231 - 0.017789j, \
                -0.0178231 + 0.017789j], 'gain': 1., 'sensitivity': 1. }
        else:
            pz = {'zeros': [-1. -1.j], 'poles': [-1. -1.j], 'gain': 1., 'sensitivity': 1.}
    
    
        return pz
    
    def resi(self, x, *args):
        f=x[0]
        h=x[1]
        sen = x[2]
        trIN = args[0]
        trOUT = args[1]
        
        paz = cornFreq2Paz(f,h)
        paz['zeros'] = [0.]
        paz['sensitivity'] = sen
        trINCP = trIN.copy()
        trINCP.trim(trINCP.stats.starttime + 50,trINCP.stats.endtime - 50)
        trINCP.detrend('constant')
        trINCP.normalize()

        trOUTsim = trOUT.copy()
        trOUTsim.simulate(paz_remove = paz)
        trOUTsim.trim(trOUTsim.stats.starttime + 50,trOUTsim.stats.endtime - 50)
        trOUTsim.detrend('constant')
        trOUTsim.normalize()

        comp = sum((trOUTsim.data - trINCP)**2)
        print(comp)
        return comp
