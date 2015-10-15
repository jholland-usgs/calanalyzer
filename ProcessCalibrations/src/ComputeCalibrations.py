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
        time = self.startdate.time
        sta = self.station
        chan = self.outChannel
        loc = self.location
        duration = self.cal_duration / 10000.0
        
        print("startdate = " + str(self.startdate))
        print("duration = " + str(duration))
	# You will want to change this to the actual network
        print ('Cal for ' + sta + ' ' + chan + ' ' + loc)
        mdgetstr = '/home/aringler/data_stuff/checkstep/./mdget.py -n IU -l ' + str(loc) + ' -c ' + str(chan) + \
            ' -s ' + str(sta) + ' -t ' + str(year) + '-' + str(day) + ' -o' + \
            ' \'instrument type\''

        print (mdgetstr)
        output = commands.getstatusoutput(mdgetstr)
        print ("output = " + str(output))
	# These might not be consistent names for sensors between networks
        try:
            output = output[1].split(',')[4]
            print ("output sensor = " + output)
        except:
            print ('We have a problem with: ' + str(sta) + ' ' + str(chan) + ' ' + str(loc))
        sensor = ''
        if 'E300' in output:
            sensor = 'STS1'
            print ('We have an E300')
        elif 'Trillium' in output:
            print ('We have an T-240')
            sensor='T240'
        elif 'STS-1' in output:
            print ('We have an STS-1')
            sensor = 'STS1'
        elif 'STS-2' in output:
            print ('We have an STS-2')
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
            
        print("trIN length = " + str(trIN.data.size))  
        print("trOUT length = " + str(trOUT.data.size))    
        
       #try:
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
        #except:
        #    print ('Unable to calculate ' + str(sta) + ' ' + str(chan) + ' ' + str(loc))
    
        #try:
        pazNOM = cornFreq2Paz(f,h)
        #poles1 = [-(h + CM.sqrt(1 - h ** 2) * 1j) * 2 * np.pi * f]
        #poles1.append(-(h - CM.sqrt(1 - h ** 2) * 1j) * 2 * np.pi * f)
        #pazNOM = {'poles': poles1, 'zeros': [0], 'gain': 1, 'sensitivity': sen}
        pazNOM['zeros']=[0.+0.j]

        pazPERT = cornFreq2Paz(bf[0],bf[1])
        #poles2 = [-(bf[0] + CM.sqrt(1 - bf[0] ** 2) * 1j) * 2 * np.pi * bf[1]]
        #poles2.append(-(bf[0] - CM.sqrt(1 - bf[0] ** 2) * 1j) * 2 * np.pi * bf[1])
        #pazPERT = {'poles': poles2, 'zeros': [0], 'gain': 1, 'sensitivity': sen}
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
        
        print("trIN length 2 = " + str(trIN.data.size))  
        print("trOUT length 2 = " + str(trOUT.data.size))    

        #if(trOUTsim.data.size > trIN.data.size):
        #    compOUT = sum((trOUTsim.data[trOUTsim.data.size - trIN.data.size:] - trIN.data)**2)
        #elif(trIN.data.size > trOUTsim.data.size):
        #    compOUT = sum((trOUTsim.data[trIN.data.size - trOUTsim.data.size:] - trIN.data)**2)
        #else:
        compOUT = sum((trOUTsim.data - trIN.data)**2)
            
        #if(trOUTsimPert.data.size > trIN.data.size):
        #    compOUTPERT = sum((trOUTsimPert.data[trOUTsimPert.data.size - trIN.data.size:] - trIN.data)**2)
        #elif(trIN.data.size > trOUTsimPert.data.size):
        #    compOUTPERT = sum((trOUTsimPert.data - trIN.data[trIN.data.size - trOUTsimPert.data.size:])**2)
        #else:
        compOUTPERT = sum((trOUTsimPert.data - trIN.data)**2)

        #compOUT = sum((trOUTsim.data - trIN.data)**2)
        #compOUTPERT = sum((trOUTsimPert.data - trIN.data)**2)
        
        #except:
        #    print ('Unable to do calculation on ' + sta + ' ' + loc + ' ' + chan)
    
        #try:
        plt.clf()
        t = numpy.arange(0,trOUTsim.stats.npts /trOUTsim.stats.sampling_rate,trOUTsim.stats.delta)
        print("t size = " + str(t[:trIN.data.size].size))
        print("trIN.data.size = " + str(trIN.data.size))
        
        #if(t.size > trIN.data.size):
        #    plt.plot(t[t.size-trIN.data.size:],trIN.data,'b',label = 'Input')
        #elif(trIN.data.size > t.size):
        #    plt.plot(t,trIN.data[trIN.data.size - t.size:],'b',label = 'Input')
        #else:
        plt.plot(t,trIN.data,'b',label = 'Input')
        
        #if(t.size > trIN.data.size):
        #    plt.plot(t[t.size - trOUTsim.data.size:],trOUTsim.data,'k',label='h=' + str(round(h,6)) + ' f=' + str(round(f,6)) + ' resi=' + str(round(compOUT,6)))
        #elif(trIN.data.size > t.size):
        #    plt.plot(t,trOUTsim.data[trOUTsim.data.size - t.size:],'k',label='h=' + str(round(h,6)) + ' f=' + str(round(f,6)) + ' resi=' + str(round(compOUT,6)))
        #else:
        plt.plot(t,trOUTsim.data,'k',label='h=' + str(round(h,6)) + ' f=' + str(round(f,6)) + ' resi=' + str(round(compOUT,6)))
        
        #if(t.size > trOUTsimPert.data.size):
        #    plt.plot(t[t.size-trOUTsimPert.data.size:],trOUTsimPert.data,'g',label = 'h=' + str(round(bf[1],6)) + ' f=' + str(round(bf[0],6))+ ' resi=' + str(round(compOUTPERT,6)))
        #elif(trOUTsimPert.data.size > t.size):
        #    plt.plot(t,trOUTsimPert.data[trOUTsimPert.data.size - t.size:],'g',label = 'h=' + str(round(bf[1],6)) + ' f=' + str(round(bf[0],6))+ ' resi=' + str(round(compOUTPERT,6)))
        #else:
        plt.plot(t,trOUTsimPert.data,'g',label = 'h=' + str(round(bf[1],6)) + ' f=' + str(round(bf[0],6))+ ' resi=' + str(round(compOUTPERT,6)))
        
        plt.xlabel('Time (s)')
        plt.ylabel('Cnts normalized')
        plt.title('Step Calibration ' + trOUT.stats.station + ' ' + str(trOUT.stats.starttime.year) + ' ' + str(trOUT.stats.starttime.julday).zfill(3))
        plt.legend()
        plt.xlim((0, 2500))
        plt.ylim((-1.5, 1.5))
        plt.show()
        plt.savefig(str(trOUT.stats.station) + str(chan) + str(loc) + str(year) + str(day) + 'step.jpg',format = "jpeg", dpi = 400)
        #except:
        #    print ('Unable to plot: ' + sta + ' ' + loc + ' ' + chan)
        print('-------------------------------------------------')

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

        #poles = [-(h + CM.sqrt(1 - h ** 2) * 1j) * 2 * np.pi * f]
        #poles.append(-(h - CM.sqrt(1 - h ** 2) * 1j) * 2 * np.pi * f)
        #paz = {'poles': poles, 'zeros': [0, 0j], 'gain': 1, 'sensitivity': sen}

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
        
        #print("trINCP length = " + str(len(trINCP)))
        #print("trOUTsim.data length = " + str(len(trOUTsim.data)))
        comp = sum((trOUTsim.data - trINCP)**2)
        print(comp)
        
        return comp
