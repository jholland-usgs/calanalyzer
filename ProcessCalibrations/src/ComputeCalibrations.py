#!/usr/bin/env python

'''
Created on Sep 18, 2015

@author: nfalco
'''

import commands
import logging
import math
import sys

import numpy
from obspy.core import read, UTCDateTime, Stream
from obspy.signal.invsim import cornFreq2Paz
from obspy.signal.invsim import pazToFreqResp
from obspy.signal.cross_correlation import xcorr, xcorr_max
import psycopg2
from scipy.optimize import fmin
import spectrum
import matplotlib.pyplot as plt


class ComputeCalibrations(object):
    '''Class containing methods to compute sine, step, and random'''
    def __init__(self, dataInLoc, dataOutLoc, startdate,
                 julianday, cal_duration, cal_id, network = '',
                 station = '', location = '', outChannel = '', signal_period = '', dbconn = None, ps = None, sentype=None):
        '''Constructor
        :param String dataInLoc: data input file path
        :param String dataOutLoc: data output file path
        :param date startdate: date of calibration
        :param int julianday: julian day of calibration
        :param float cal_duration: duration of the calibration
        :param int cal_id: database record calibration id
        :param String network: network that calibrated sensor belongs to
        :param String station: station that the calibrated sensor belongs to
        :param String location: location that the calibrated sensor belongs to
        :param String outChannel: the output channel to calibrate (e.g. BHZ, LHZ, etc.)
        :param int signal_period: the period of the sine calibration signal [sine calibration only]
        :param object dbconn: database connection string object
        :param object ps: dictionary containing pole values for the given seismometer response. [sine calibration only]
        :param String sentype: type of sensor being calibrated (e.g. STS-2) [optional]
        '''
        self.dataInLoc = dataInLoc  # Location where the input data is located
        # Location where the output seed data is located
        self.dataOutLoc = dataOutLoc
        self.startdate = startdate  # Start data for the calibration
        self.julianday = julianday
        # Duration of the calibration in milliseconds
        self.cal_duration = cal_duration
        self.cal_id = cal_id  # Database primary key of the calibration
        self.network = network
        self.station = station
        self.location = location
        # Channel for which the calibration was calculated (eg. BHZ, BH1, BH2,
        # etc.)
        self.outChannel = outChannel
        self.sentype = sentype  # Manual override for sensor type
        self.dbconn = dbconn  # Database connection object
        self.ps = ps
        self.signal_period = signal_period #period of sine cal
        # Setup logging for calibrations
        self.stepcal_logger = logging.getLogger('ComputeCalibrations.stepcal')
        self.sinecal_logger = logging.getLogger('ComputeCalibrations.sinecal')

    def computeSineCal(self):
        '''Computes the Sine Calibration rms input, rms output, and coil constant'''
        debug = False
        try:
            # determine sensor type
            if self.sentype is None:
                self.sentype = self._determineSensorType()
                
            if(self.dbconn is not None):
                # divide by 10000 when getting the cal_duration from the database
                self.cal_duration = self.cal_duration / 10000.0
            else:
                self.cal_duration = self.cal_duration
            
            # Read in BH and BC
            dataIN = read(self.dataInLoc)
            dataOUT = read(self.dataOutLoc)
            
            # Convert start date to UTC
            stime = UTCDateTime(str(self.startdate))
    
            #get poles and zeros of 
            paz = self.ps.get_paz(str(self.network)+'.'+str(self.station)+'.'+
                        str(self.location)+'.'+str(dataOUT[0].stats.channel), stime)
            # Switch to acceleration
            paz['zeros'].remove(0.j)
            
            # Trim data to only grab the sine calibration
            dataIN.trim(starttime=stime, endtime=stime + self.cal_duration)
            dataOUT.trim(starttime=stime, endtime=stime + self.cal_duration)
            
            # Convert input and output to volts
            dataINVolts = dataIN[0]
            dataINVolts.data = dataIN[0].data / float(paz['digitizer_gain'])/4. #remove digitizer gain from input signal
            dataOUTVolts = dataOUT[0]
            dataOUTVolts.simulate(paz_remove = paz, remove_sensitivity=True) #removes response. Units now in m/s
            # convert units to volts by mutilplying by sensitivity (volt/(m/s))
            dataOUTVolts.data = numpy.multiply(dataOUTVolts.data, float(str(paz['seismometer_gain']))) #units now in volts
    
            # Add taper and switch to zerophase bandpass
            dataOUTVolts.taper(0.05)
            dataINVolts.taper(0.05)
            
            if self.signal_period == 1.0:
                dataOUTVolts.filter('bandpass',freqmin=0.5, freqmax=2.0, zerophase=True, corners=2)
                dataINVolts.filter('bandpass',freqmin=0.5, freqmax=2.0, zerophase=True, corners=2)
            else:
                dataOUTVolts.filter('bandpass',freqmin=1./(1.5*250.), freqmax=1./(10.), zerophase=True, corners=2)
                dataINVolts.filter('bandpass',freqmin=1./(1.5*250.), freqmax=1./(10.), zerophase=True, corners=2)
                
            # Trim the start and end of the data 
            dataINVolts.trim(starttime=(dataINVolts.stats.starttime+200.), endtime=(dataINVolts.stats.endtime-200.))
            dataOUTVolts.trim(starttime=(dataOUTVolts.stats.starttime+200.), endtime=(dataOUTVolts.stats.endtime-200.))
            
            # Calculate RMS of both traces and divide
            dataINRMS = math.sqrt(sum(numpy.square(dataINVolts.data)) / dataINVolts.stats.npts)
            dataOUTRMS = math.sqrt(sum(numpy.square(dataOUTVolts.data)) / dataOUTVolts.stats.npts)
            
            # Determine the cross correlation and flip signal if it is negative
            corr = xcorr(dataINVolts.data, dataOUTVolts.data, 1000, full_xcorr=True)
            shift, corr = xcorr_max(corr[2])
            if corr < 0:
                dataOUTVolts.data = -dataOUTVolts.data
            try:
                plt.clf()
                t = numpy.arange(
                    0, dataOUTVolts.stats.npts / dataOUTVolts.stats.sampling_rate, dataOUTVolts.stats.delta)
                
                fig = plt.figure(1)
                fntsize = 10
                #top plot
                ax1 = fig.add_subplot(211)
                ax1.set_title(str(self.signal_period) + ' sec period Sine Calibration \n' + 
                    str(dataOUTVolts.stats.station) + ' ' + self.location + ' ' + self.outChannel + ' ' + 
                    str(dataOUTVolts.stats.starttime.year) + ' ' + str(dataOUTVolts.stats.starttime.julday).zfill(3) + 
                    '\nInput RMS = ' + str(round(dataINRMS, 4)) + ' volts, Output RMS = ' + str(round(dataOUTRMS,4)) + ' volts, Correlation = ' + 
                    str(round(corr,4)), fontsize=fntsize)
                ax1.set_xlabel('Time (s)', fontsize=fntsize)
                ax1.set_ylabel('Volts', fontsize=fntsize)
                plt.plot(t, dataINVolts.data, 'b', label='input')
                plt.plot(t, dataOUTVolts.data, 'k', label='output')
                plt.legend(prop={'size': fntsize})
                #bottom plot
                ax2 = fig.add_subplot(212)
                ax2.set_title('Calibration Input to Output Ratio Sine', fontsize=10)
                ax2.set_xlabel('Cal output (V)', fontsize=fntsize)
                ax2.set_ylabel('Cal input (V)', fontsize=fntsize)
                
                plt.plot(dataOUTVolts.data, dataINVolts.data, 'b')
                #add space between plots
                fig.subplots_adjust(hspace=.5)
                #save figure
                plt.savefig('temp/' + str(self.startdate.year) + '_' + str(self.julianday) + '_'+ str(dataOUTVolts.stats.station) + '_' + str(self.location) + '_' + str(self.outChannel) +
                    '_' + str(self.startdate.time()) + 'sine.png', format="png", dpi=200)
                if debug:
                    plt.show()
                plt.close()
            except:
                if(self.dbconn is not None):
                    if debug:
                        print 'network = ' + str(self.network)
                        print 'station = ' + str(self.station)
                        print 'sensor = ' + str(self.sensor)
                        print 'location = ' + str(self.location)
                        print 'channel = ' + str(self.channel)
                    if (self.network is not None and self.station is None and self.sentype is not None 
                        and self.location is not None and self.outChannel is not None):
                        self.stepcal_logger.error('Unable to plot {' +
                                                  'network = ' + str(self.network) +
                                                  ', station = ' + str(self.station) +
                                                  ', sensor = ' + str(self.sentype) +
                                                  ', location = ' + str(self.location) +
                                                  ', channel = ' + str(self.outChannel) +
                                                  '}')
                    else:
                        self.stepcal_logger.error('Unable to plot cal for {' +
                          'data input = ' + str(self.dataInLoc) +
                          ', data output = ' + str(self.dataOutLoc) +
                          '}')
                else:
                    self.stepcal_logger.error('(Manual Override) Unable to make plot for input file ' + str(
                        self.dataInLoc) + ' and output file ' + str(self.dataOutLoc))
    
            if(self.dbconn is not None):
                # Write results to database
                cur = self.dbconn.cursor()
                cur.execute("""INSERT INTO tbl_310calresults (fk_calibrationid,
                            input_rms, output_rms, outchannel, coil_constant)
                            VALUES (""" + "'" + str(self.cal_id) + "', '" +
                            str(dataINRMS) + "', '" + str(dataOUTRMS) +
                            "', '" + str(self.outChannel) + "', '" +
                            str(dataINRMS / dataOUTRMS) + "')")
                self.dbconn.commit()
                cur.close()
            else:
                print('input rms = ' + str(dataINRMS) +
                      ', output rms = ' + str(dataOUTRMS) +
                      ', coil constant = ' + str(dataINRMS / dataOUTRMS))
        except:
            self.sinecal_logger.error("Unexpected error:", str(sys.exc_info()[0]))


    def computeStepCal(self):
        '''Computes the nominal, best fit, and actual step calibration'''
        # cal duration needs to be divided by 10000 for step cals only.  This
        # only applies for when you are reading the cal duration from the
        # database.
        if(self.dbconn is not None):
            # divide by 10000 when getting the cal_duration from the database
            duration = self.cal_duration / 10000.0
        else:
            duration = self.cal_duration

        # Determine the type of sensor from the metadata
        sensor = self._determineSensorType()

        # ignores every location except for Z for triaxial STS-2s
        if((self.dbconn is not None) and ("Z" not in self.outChannel) and
           (sensor == "STS-2HG" or sensor == "STS-4B" or sensor == "STS-2")):
            print("Skipped " + str(self.outChannel) + ' ' + sensor)

        # get the poles values for the sensor type
        pz = self._pzvals(sensor)

        # read data for the calibration
        try:
            stOUT = Stream()
            stime = UTCDateTime(self.startdate) - 5 * 60
            stOUT = read(
                self.dataOutLoc, starttime=stime,
                endtime=stime + duration + 5 * 60 + 900
            )
            stOUT.merge()
            stIN = read(
                self.dataInLoc, starttime=stime,
                endtime=stime + duration + 5 * 60 + 900
            )
            stIN.merge()
            trIN = stIN[0]
            trOUT = stOUT[0]
            trOUT.filter('lowpass', freq=.1)
            trIN.filter('lowpass', freq=.1)
            trIN.detrend('constant')
            trIN.normalize()
            trOUT.detrend('constant')
            trOUT.normalize()
            temp = trOUT.copy()
            temp.trim(endtime=stime + int(duration / 2.))
            if temp.max() < 0.0:
                trOUT.data = -trOUT.data
        except:
            if(self.dbconn is not None):
                self.stepcal_logger.error('Unable to read data for {' +
                                          'network = ' + self.network +
                                          ', station = ' + self.station +
                                          ', sensor = ' + str(sensor) +
                                          ', location = ' + str(self.location) +
                                          ', channel = ' + str(self.outChannel) +
                                          '}')
            else:
                self.stepcal_logger.error('''(Manual Override) Unable read data
                                          for manual input file ''' +
                                          str(self.dataInLoc) +
                                          ' and output file ' +
                                          str(self.dataOutLoc))
        try:
            # compute corner (cutoff) frequency
            f = 1. / (2 * math.pi / abs(pz['poles'][0]))
            # compute damping ratio
            h = abs(pz['poles'][0].real) / abs(pz['poles'][0])
            sen = 10.0

            print (
                'Using: h=' + str(h) + ' f=' + str(f) + ' sen = ' + str(sen))

            x = numpy.array([f, h, sen])
            try:
                # compute best fit
                bf = fmin(self._resi, x, args=(trIN, trOUT),
                          xtol=10 ** -8, ftol=10 ** -3, disp=False)
            except:
                bf = x

        except:
            if(self.dbconn is not None):
                self.stepcal_logger.error('Unable to calculate {' +
                                          'network = ' + self.network +
                                          ', station = ' + self.station +
                                          ', sensor = ' + str(sensor) +
                                          ', location = ' + str(self.location) +
                                          ', channel = ' + str(self.outChannel) +
                                          '}')
            else:
                self.stepcal_logger.error('''(Manual Override) Unable to
                                          perform corner freq, damping ratio,
                                          and best fit calculations for input
                                          file ''' + str(self.dataInLoc) +
                                          ' and output file ' +
                                          str(self.dataOutLoc))
        try:
            pazNOM = cornFreq2Paz(f, h)
            pazNOM['zeros'] = [0. + 0.j]

            pazPERT = cornFreq2Paz(bf[0], bf[1])
            pazPERT['zeros'] = [0]

            trOUTsimPert = trOUT.copy()
            trOUTsimPert.simulate(paz_remove=pazPERT)
            trOUTsimPert.trim(
                trOUTsimPert.stats.starttime + 50, trOUTsimPert.stats.endtime - 50)
            trOUTsimPert.detrend('constant')
            trOUTsimPert.normalize()

            trOUTsim = trOUT.copy()

            trOUTsim.simulate(paz_remove=pazNOM)
            trOUTsim.trim(
                trOUTsim.stats.starttime + 50, trOUTsim.stats.endtime - 50)
            trOUTsim.detrend('constant')
            trOUTsim.normalize()

            trIN.trim(trIN.stats.starttime + 50, trIN.stats.endtime - 50)
            trIN.detrend('constant')
            trIN.normalize()

            compOUT = sum((trOUTsim.data - trIN.data) ** 2)
            compOUTPERT = sum((trOUTsimPert.data - trIN.data) ** 2)
        except:
            if(self.dbconn is not None):
                self.stepcal_logger.error('Unable to do calculation for {' +
                                          'network = ' + self.network +
                                          ', station = ' + self.station +
                                          ', sensor = ' + str(sensor) +
                                          ', location = ' + str(self.location) +
                                          ', channel = ' + str(self.outChannel) +
                                          '}')
            else:
                self.stepcal_logger.error('''(Manual Override) Unable to
                                         perform poles calculation or input
                                         file ''' + str(self.dataInLoc) +
                                          ' and output file ' +
                                          str(self.dataOutLoc))
        try:
            # create a plot for the step calibration and save it to the ./temp
            # directory.  This directory will be deleted when the program is
            # finished running.
            plt.clf()
            t = numpy.arange(
                0, trOUTsim.stats.npts / trOUTsim.stats.sampling_rate, trOUTsim.stats.delta)
            plt.plot(t, trIN.data, 'b', label='input')
            plt.plot(t, trOUTsim.data, 'k', label='h=' + str(round(h, 6)) +
                     ' f=' + str(round(f, 6)) + ' resi=' + str(round(compOUT, 6)))
            plt.plot(t, trOUTsimPert.data, 'g', label='h=' + str(round(bf[1], 6)) + ' f=' + str(
                round(bf[0], 6)) + ' resi=' + str(round(compOUTPERT, 6)))
            plt.xlabel('Time (s)')
            plt.ylabel('Cnts normalized')
            plt.title('Step Calibration ' + trOUT.stats.station + ' ' + str(
                trOUT.stats.starttime.year) + ' ' + str(trOUT.stats.starttime.julday).zfill(3))
            plt.legend(prop={'size': 6})
            plt.savefig('temp/' + str(trOUT.stats.station) + str(self.outChannel) + str(self.location) +
                        str(self.startdate.year) + str(self.julianday) + 'step.png', format="png", dpi=400)
            plt.close()
        except:
            if(self.dbconn is not None):
                self.stepcal_logger.error('Unable to plot {' +
                                          'network = ' + self.network +
                                          ', station = ' + self.station +
                                          ', sensor = ' + str(sensor) +
                                          ', location = ' + str(self.location) +
                                          ', channel = ' + str(self.outChannel) +
                                          '}')
            else:
                self.stepcal_logger.error('(Manual Override) Unable to make plot for input file ' + str(
                    self.dataInLoc) + ' and output file ' + str(self.dataOutLoc))
        if(self.dbconn is not None):
            try:
                plt.close()
                # insert results into the database
                fin = open('temp/' + str(trOUT.stats.station) + str(self.outChannel) + str(
                    self.location) + str(self.startdate.year) + str(self.julianday) + 'step.png', 'rb')
                imgdata = fin.read()
                cur = self.dbconn.cursor()
                cur.execute('''INSERT INTO tbl_300calresults (fk_calibrationid,
                              nominal_cornerfreq, nominal_dampingratio, nominal_resi,
                              fitted_cornerfreq, fitted_dampingratio, fitted_resi,
                              outchannel, stepcal_img)
                              VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                            [self.cal_id, round(f, 6), round(h, 6),
                             round(compOUT, 6), round(bf[0], 6),
                             round(bf[1], 6), round(compOUTPERT, 6),
                             str(self.outChannel), psycopg2.Binary(imgdata)])
                self.dbconn.commit()
                cur.close()
            except:
                self.stepcal_logger.error('Unable to insert into database for {' +
                                          'network = ' + self.network +
                                          ', station = ' + self.station +
                                          ', sensor = ' + str(sensor) +
                                          ', location = ' + str(self.location) +
                                          ', channel = ' + str(self.outChannel) +
                                          '}')

        else:
            try:
                print('nominal corner freq = ' + str(round(f, 6)) +
                      ', nominal damping ratio = ' + str(round(h, 6)) +
                      ', nominal best fit = ' + str(round(compOUT, 6)) +
                      ', fitted corner freq = ' + str(round(bf[0], 6)) +
                      ', fitted damping ratio = ' + str(round(bf[1], 6)) +
                      ', pert best fit ' + str(round(compOUTPERT, 6)))
                plt.show()
                plt.close()
            except:
                print(
                    '(Manual Override) Error displaying calculation results.')

    def computeRandomCal(self):
        '''Computes the nominal, actual, and best fit amplitude and phase responses of an instrument 
        '  for a random calibration.
        '''
        debug = False
        if(self.dbconn is not None):
            # divide by 10000 when getting the cal_duration from the database
            duration = self.cal_duration / 10000.0
        else:
            duration = self.cal_duration
        if debug:
            print 'cal duration = ' + str(duration)
        # read data for the random calibration
        stime = UTCDateTime(self.startdate)
        stOUT = read(
            self.dataOutLoc, starttime=stime, endtime=stime + duration)
        stOUT.merge()
        stIN = read(self.dataInLoc, starttime=stime, endtime=stime + duration)
        stIN.merge()
        trIN = stIN[0]
        trOUT = stOUT[0]

        if debug:
            trIN.plot()
            trOUT.plot()

        # remove the linear trend from the data
        trIN.split().detrend('constant')
        trOUT.split().detrend('constant')
        samplerate = trOUT.stats.sampling_rate
        segLength = int(
            math.pow(2, math.floor(math.log(math.floor(len(trIN.data) / 1.3), 2))))
        offset = int(0.8 * segLength)
        cnt = 0

        if debug:
            print 'Here is the segment length: ' + str(segLength)
            print 'Here is the offset: ' + str(offset)
        runningTotal = numpy.zeros(segLength)
        numSegments = 0
        [tapers, eigen] = spectrum.dpss(segLength, 12, 12)
        while (cnt + segLength < len(trIN.data)):
            x = trIN.data[cnt:cnt + segLength]
            y = trOUT.data[cnt:cnt + segLength]
            # perform the multi-taper method on both the input and output
            # traces
            pxx = self._pmtm(
                x, x, e=tapers, v=eigen, NFFT=segLength, show=False)

            pxy = self._pmtm(
                x, y, e=tapers, v=eigen, NFFT=segLength, show=False)

            # determine the frequency response by dividing the output by the
            # input
            res = numpy.divide(pxy, pxx)
            # create a running total of all responses
            runningTotal = numpy.add(runningTotal, res)
            if(cnt + segLength > len(trIN.data)):
                cnt = len(trIN.data) - segLength
            else:
                cnt = cnt + (segLength - offset)
            if debug:
                print 'Here is the cnt: ' + str(cnt)
            numSegments = numSegments + 1
        # find the average of segments
        res = runningTotal / numSegments

        # generate the frequency array
        freq = numpy.multiply(numpy.fft.fftfreq(len(res)), samplerate)

        # determine sensor type
        if self.sentype is None:
            self.sentype = self._determineSensorType()

        # determine the response based off of the poles and zeros values
        resPaz = self._getRespFromModel(
            self._pzvals(self.sentype), len(res), trOUT.stats.delta)

        # compute best fit
        resBfPolesList = fmin(self._resiFreq,  numpy.array(self._pazDictToList(self._pzvals(self.sentype))), args=(res, samplerate, freq),
                  xtol=10 ** -8, ftol=10 ** -3, disp=False)
        resBfPoles = self._pazListToDict(resBfPolesList)
        resBf = self._getRespFromModel(
            resBfPoles, len(res), trOUT.stats.delta)

        # only grab positive frequencies
        freq = freq[freq > 0]
        # get the index of the frequency closest to 20 seconds period (0.05 Hz)
        freq20Array = freq[(freq >= (1. / 20.))]
        min20Freq = numpy.min(freq20Array)
        freq20Index = numpy.where(freq == min20Freq)[0]

        # get the index of the frequency closest to 1000 seconds period (0.001
        # Hz)
        freq1000Array = freq[(freq >= (1. / 1000.))]
        min1000Freq = numpy.min(freq1000Array)
        freq1000Index = numpy.where(freq == min1000Freq)[0]

        # limit to data between 20s and 1000s period
        freq = freq[freq1000Index: freq20Index]
        
        res = res[freq1000Index: freq20Index]
        resPaz = resPaz[freq1000Index: freq20Index]
        resBf = resBf[freq1000Index: freq20Index]
        
        #convert to degrees
        res = res * (2. * math.pi * freq)
        resPaz = resPaz * (2. * math.pi * freq)
        resBf = resBf * (2. * math.pi * freq)

        # get index where frequency closest to 50 seconds (0.02 Hz)
        freq50Array = freq[(freq >= (1. / 50.))]
        min50Freq = numpy.min(freq50Array)
        res50Index = numpy.where(freq == min50Freq)[0]

        res, resPhase = self._respToFAP(res, res50Index)
        resPaz, resPazPhase = self._respToFAP(resPaz, res50Index)
        resBf, resBfPhase = self._respToFAP(resBf, res50Index)

        # calculate the free period
        fp = 2 * math.pi / abs(numpy.min(self._pzvals(self.sentype)['poles']))
        # determine the damping ratio of the signal
        damping = abs(
            numpy.real(numpy.min(self._pzvals(self.sentype)['poles'])) / (2 * math.pi / fp))

        fig = plt.figure(figsize=(14,10))
        ax = fig.add_subplot(121)
        ax.semilogx(numpy.divide(1, freq), res, label='Actual')
        ax.semilogx(numpy.divide(1, freq), resPaz, label='Nominal')
        ax.semilogx(numpy.divide(1, freq), resBf, label='Best Fit')
        ax.set_xlabel('Period (seconds)')
        ax.set_ylabel('Amplitude [DB]')
        ax.legend(loc='lower left', ncol=2, borderaxespad=0.)

        ax = fig.add_subplot(122)
        ax.semilogx(numpy.divide(1, freq), resPhase, label='Actual')
        ax.semilogx(numpy.divide(1, freq), -resPazPhase, label='Nominal')
        ax.semilogx(numpy.divide(1, freq), -resBfPhase, label='Best Fit')
        ax.set_xlabel('Period (seconds)')
        ax.set_ylabel('Phase [radian]')
        plt.legend(loc='lower left', ncol=2, borderaxespad=0.)
        plt.subplots_adjust(wspace=0.3, top=0.85)
        
        title = 'Frequency Response of a ' + self.sentype + ' Seismometer for \n Station = ' \
            + self.network + '_' + self.station + ', Location = ' + self.location \
            + ', Channel = ' + self.outChannel + ', Start-time = ' + str(self.startdate) \
            + '\nh = ' + str(damping) + ', fp= ' + str(fp)
        plt.suptitle(title, fontsize=11)
        plt.savefig('temp/' + str(trOUT.stats.station) + str(self.outChannel) + str(self.location) +
            str(self.startdate.year) + str(self.julianday) + 'random.png', format="png", dpi=400)
        if debug:
            plt.show()
        plt.close()
        if(self.dbconn is not None):
            # insert results into the database
            fin = open('temp/' + str(trOUT.stats.station) + str(self.outChannel) + str(
                self.location) + str(self.startdate.year) + str(self.julianday) + 'random.png', 'rb')
            imgdata = fin.read()
            # Write results to database
            cur = self.dbconn.cursor()
            cur.execute("""INSERT INTO tbl_320calresults (fk_calibrationid,
                        damping, freeperiod, randomcal_img)
                        VALUES (%s,%s,%s,%s)""",
                            [self.cal_id, round(damping, 6), round(fp, 6),
                             psycopg2.Binary(imgdata)])
            self.dbconn.commit()
            cur.close()
        else:
            print('Unable to insert random calibration into the database.')

    def _getRespFromModel(self, pazModel, nfft, delta):
        '''Returns the frequency response given a dictionary of poles and zeros without any normalization
        :param dictionary pazModel: dictionary or poles and zeros
        :param int nfft:
        :param delta: delta T between samples
        '''
        # This function returns the response without normalization
        resp = pazToFreqResp(pazModel['poles'], pazModel['zeros'], 1, delta,
                             nfft, freq=False)
        return resp
    
    def _pzvals(self, sensor):
        '''Given a sensor type returns a dictionary of poles and zeros
        :param string sensor: sensor type. Choose from: STS-1, STS-2, STS-2GH, T-120, T-240, CMG-3T, KS-54000
        '''
        # get the instrument values for a given type of seismometer
        if sensor == 'STS-1':
            pz = {'zeros': [0.],
                  'poles': [-0.01234 - 0.01234j,
                            - 0.01234 + 0.01234j],
                  'gain': 1.,
                  'sensitivity': 1.}
        elif sensor == 'STS-2':
            pz = {'zeros': [0.],
                  'poles': [-0.035647 - 0.036879j,
                            - 0.035647 + 0.036879j], 'gain': 1., 'sensitivity': 1.}
        elif sensor == 'STS-2HG':
            pz = {'gain': 5.96806 * 10 ** 7, 'zeros': [0, 0],
                  'poles': [-0.035647 - 0.036879j,
                            - 0.035647 + 0.036879j, -251.33,
                            -131.04 - 467.29j, -131.04 + 467.29j],
                  'sensitivity': 3.355500 * 10 ** 10}
        elif sensor == 'T-120':
            pz = {'gain': 8.318710 * 10 ** 17,
                  'zeros': [0 + 0j, 0 + 0j, -31.63 + 0j,
                            - 160.0 + 0j, -350.0 + 0j, -3177.0 + 0j],
                  'poles': [-0.036614 + 0.037059j,
                            - 0.036614 - 0.037059j, -32.55 +
                            0j, -142.0 +
                            0j, -364.0 +
                            404.0j,
                            - 364.0 - 404.0j, -1260.0 + 0j, -4900.0 +
                            5204.0j, -4900.0 -
                            5204.0j,
                            - 7100.0 + 1700.0j, -7100.0 - 1700.0j],
                  'sensitivity': 2.017500 * 10 ** 9}
        elif sensor == 'T-240':
            pz = {'zeros': [0.],
                  'poles': [-0.0178231 - 0.017789j,
                            - 0.0178231 + 0.017789j],
                  'gain': 1.,
                  'sensitivity': 1.}
        elif sensor == 'CMG-3T':
            pz = {'gain': 5.71508 * 10 ** 8,
                  'zeros': [0, 0],
                  'poles': [-0.037008 - 0.037008j,
                            - 0.037008 + 0.037008j,
                            -502.65, -1005.0, -1131.0],
                  'sensitivity': 3.3554 * 10 ** 10}
        elif sensor == 'KS-54000':
            pz = {'gain': 86298.5,
                  'zeros': [0, 0],
                  'poles': [-59.4313,
                            - 22.7121 + 27.1065j, -22.7121 + 27.1065j, -0.0048004, -0.073199],
                  'sensitivity': 3.3554 * 10 ** 9}
        elif sensor == 'KS-54000':
            pz = {'gain': 86298.5,
                  'zeros': [0, 0],
                  'poles': [-59.4313,
                            - 22.7121 + 27.1065j, -22.7121 + 27.1065j, -0.0048004, -0.073199],
                  'sensitivity': 3.3554 * 10 ** 9}
        else:
            pz = {'zeros': [-1. - 1.j],
                  'poles': [-1. - 1.j],
                  'gain': 1.,
                  'sensitivity': 1.}

        return pz
    
    def _respToFAP(self, resp, norm):
        '''Returns a normalized amplitude response and phase response in dB
        :param numpy array resp: Array of response signal to be converted
        :param int norm: index of the response array for the value to be normalized by. Usually the index of the array
                    closest to 50 seconds period.
        '''
        # This function returns the phase in degrees and the amp in dB
        # Convert the amplitude response to dB
        respAmp = 20. * numpy.log10(abs(resp))
        respAmp = respAmp - respAmp[norm]

        # Convert the phase response to degrees
        respPhase = numpy.unwrap(numpy.angle(resp)) * 180 / math.pi
        respPhase = respPhase - respPhase[norm]

        return respAmp, respPhase

    def _pazDictToList(self, pazDict):
        '''Converts dictionary of poles and zeros into a list of real and imaginary values seperated by sys.maxint
        :param dictionary pazDict: dictonary of poles and zeros.
        '''
        poles = pazDict['poles']
        zeros = pazDict['zeros'] 
        polesList = []
        absValuePolesList = []
        for pole in poles:
            polesList.append(pole.real)
            polesList.append(pole.imag)
            absValuePolesList.append(abs(pole))
        zerosList = []
        for zero in zeros:
            zerosList.append(zero.real)
            zerosList.append(zero.imag)
        return polesList + [sys.maxint] +  zerosList
    
    def _pazListToDict(self, pazList):
        '''Converts list of real and imaginary values to ad dictionary of complex poles and zeros
        :param list pazList: List of the nominal poles and zeros values split by sys.maxint. Each complex
                    value is separated into its real and imaginary components. 
                    i.e. [pole real val, pole imaginary val, ... , sys.maxint, zero real val, pole imaginary val, ... ]
        '''
        poles, zeros = self._isplit(pazList, sys.maxint)
        cmplxPoles = []
        i = 0
        while i+1 < len(poles):
            cmplxPoles.append( (poles[i] + (poles[i+1] * 1j)) ) 
            i = i + 2
        cmplxZeros = []
        i = 0
        while i+1 < len(zeros):
            cmplxZeros.append( (zeros[i] + (zeros[i+1] * 1j)) ) 
            i = i + 2
        paz = {'zeros' : cmplxZeros, 'poles' : cmplxPoles}
        return paz
    
    def _isplit(self, iterable,splitter):
        '''Splits a list of values into two separate list based on one splitter value
    '   '  e.g. is splitter = sys.maxint then [8, 2j, sys.maxint, 3, 5j] becomes two list containg [8, 2j] [3, 5j]
        :param list iterable: list of values
        :param splitter: value to split list by
        '''
        polesList = []
        zerosList = []

        for item in iterable:
            if item != splitter:
                polesList.append(item)
            else:
                break

        reversed_arr = iterable[::-1]
        for item in reversed_arr:
            if item != splitter:
                zerosList.append(item)
            else:
                break
        return polesList, zerosList
    
    def _resi(self, x, *args):
        '''Residual function for computing step calibration. Calculates estimated instrument free 
        '  period, damping, and sensitivity. 
        :param numpy array x: Containing original guess for free period, damping, and sensitivity
        :param tuple *args: tuple containing the step calibration input signal and output signal.     
        '''
        f = x[0]
        h = x[1]
        sen = x[2]
        trIN = args[0]
        trOUT = args[1]

        paz = cornFreq2Paz(f, h)
        paz['zeros'] = [0.]
        paz['sensitivity'] = sen
        trINCP = trIN.copy()
        trINCP.trim(trINCP.stats.starttime + 50, trINCP.stats.endtime - 50)
        trINCP.detrend('constant')
        trINCP.normalize()

        trOUTsim = trOUT.copy()
        trOUTsim.simulate(paz_remove=paz)
        trOUTsim.trim(
            trOUTsim.stats.starttime + 50, trOUTsim.stats.endtime - 50)
        trOUTsim.detrend('constant')
        trOUTsim.normalize()

        comp = sum((trOUTsim.data - trINCP) ** 2)
        return comp
    
    def _resiFreq(self, x, *args):
        '''Residual method used to calculate best fit estimated poles and zeros values for the random calibration
        :param numpy array x: Array of the nominal poles and zeros values split by sys.maxint. Each complex
                    value is separated into its real and imaginary components. 
                    i.e. [pole real val, pole imaginary val, ... , sys.maxint, zero real val, pole imaginary val, ... ]
        :param tuple *args: tuple containing the acutal response and the sample rate.         
        '''
        paz = self._pazListToDict(x)
        respActual = args[0][1:] #skip 0 term
        samplerate = args[1]
        freq = args[2]
        #compute response from given poles and zeros
        respPaz = abs(self._getRespFromModel(paz, len(respActual) * 2, 1. / samplerate))[1:] #skip zero term to avoid taking log10(0)
        respPAZ = 20. * numpy.log10(respPaz) #convert to dB
        respActual = 20. * numpy.log10(respActual)
        #convert actual response to dB
        #get the maximum magnitude to normalize by
        pazMaxMag = numpy.amax(respPAZ)
        
        #phase angle arrays
        respActualAngle = numpy.arctan(respActual)
        respPAZAngle = numpy.arctan(respPAZ)
        
        deltaFreqList = []
        deltaFreqIndex = 0
        while deltaFreqIndex + 1 < len(freq):
            deltaFreqList.append(freq[deltaFreqIndex + 1] - freq[deltaFreqIndex])
            deltaFreqIndex = deltaFreqIndex + 1
        deltaFreqArray = numpy.array(deltaFreqList)

        #get maximum phase angle to normalize by
        pazMaxTheta = numpy.amax(numpy.arctan(respPaz))
        comp = numpy.sum( ( ((respActual - respPAZ) / pazMaxMag) ** 2 ) + ( ((respActualAngle - respPAZAngle) / pazMaxTheta) ** 2 ) * deltaFreqArray)
        print comp
        return comp

    def _determineSensorType(self):
        '''Returns the sensor type for a given station location/channel'''
        if(self.dbconn is not None):
            # Remove this hard coded locations
            mdgetstr = '/home/nfalco/calanalyzer/ProcessCalibrations/src/./mdget.py -n ' + str(self.network) + \
                ' -l ' + str(self.location) + ' -c ' + str(self.outChannel) + \
                ' -s ' + str(self.station) + ' -t ' + str(self.startdate.year) + \
                '-' + str(self.julianday) + ' -o \'instrument type\''
            print mdgetstr
            output = commands.getstatusoutput(mdgetstr)

            # These might not be consistent names for sensors between networks
            try:
                # extract the sensor data from the metadata output
                output = output[1].split(',')[4]
            except:
                self.stepcal_logger.error('Unable to acquire sensor information for {' +
                                          'network = ' + self.network +
                                          ', station = ' + self.station +
                                          ', location = ' + str(self.location) +
                                          ', channel = ' + str(self.outChannel) +
                                          '}')
        else:
            output = self.sentype

        sensor = ''
        if ('T-240' in output) or ('Trillium 240' in output):
            sensor = 'T-240'
        elif ('T-120' in output) or ('T120' in output) or ('TRILLIUM_120' in output):
            sensor = 'T-120'
        elif ('CMG-3T' in output) or ('CMG3T' in output) or ('CMG3-T' in output):
            sensor = 'CMG-3T'
        elif 'STS-2HG' in output:
            sensor = 'STS2-HG'
        elif 'STS-4B' in output:
            sensor = 'STS-4B'
        elif 'KS-54000' in output:
            sensor = 'KS-54000'
        elif '151-120' in output:
            sensor = '151-120'
        elif ('STS-1' in output) or ('STS1' in output) or ('E300' in output):
            sensor = 'STS-1'
        elif ('STS-2' in output) or ('STS2' in output):
            sensor = 'STS-2'

        return sensor

    def _pmtm(self, x, y, NW=None, k=None, NFFT=None, e=None, v=None, method='eigen', show=True):
        '''Multitapering sepectral estimation method used for random calibration calculations.
        :param array x: the data
        :param array y: the data
        :param float NW: The time half bandwidth parameter (typical values are 2.5,3,3.5,4).
                 Must be provided otherwise the tapering windows and eigen values
                 (outputs of dpss) must be provided.
        :param int k: uses the first k Slepian sequences. If *k* is not provided, *k* is set to *NW*2*.
        :param NW
        :param e: the matrix containing the tapering windows
        :param v: the window concentrations (eigenvalues)
        :param str method: set how the eigenvalues are used. Must be in ['unity', 'adapt', 'eigen']
        :param bool show: plot results
        '''
        debug = True
        assert method in ['adapt', 'eigen', 'unity']

        N = len(x)
        # if dpss not provided, compute them
        if e is None and v is None:
            if NW is not None:
                [tapers, eigenvalues] = spectrum.dpss(N, NW, k=k)
            else:
                raise ValueError("NW must be provided (e.g. 2.5, 3, 3.5, 4")
        elif e is not None and v is not None:
            if debug:
                print 'Using given tapers.'
            eigenvalues = v[:]
            tapers = e[:]
        else:
            raise ValueError(
                "if e provided, v must be provided as well and viceversa.")
        # length of the eigen values vector to be used later
        nwin = len(eigenvalues)

        # set the NFFT
        if NFFT is None:
            NFFT = max(256, 2 ** spectrum.nextpow2(N))
        # si nfft smaller than N, cut otherwise add zero.
        # compute
        if method == 'unity':
            weights = numpy.ones((nwin, 1))
        elif method == 'eigen':
            # The S_k spectrum can be weighted by the eigenvalues, as in Park
            # et al.
            weights = numpy.array([_x / float(i + 1)
                                   for i, _x in enumerate(eigenvalues)])
            weights = weights.reshape(nwin, 1)

        xin = numpy.fft.fft(numpy.multiply(tapers.transpose(), x), NFFT)
        yin = numpy.fft.fft(numpy.multiply(tapers.transpose(), y), NFFT)

        Sk = numpy.multiply(xin, numpy.conj(yin))
        Sk = numpy.mean(Sk * weights, axis=0)

        # clf(); p.plot(); plot(arange(0,0.5,1./512),20*log10(res[0:256]))
        if show is True:
            spectrum.semilogy(Sk)
        return Sk
