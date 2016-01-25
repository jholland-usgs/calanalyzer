'''
Created on Sep 18, 2015

@author: nfalco
'''

import argparse
import yaml

class ParseConfig(object):
    
    def __init__(self):
        arguments = self._getArguments() #gets arguments from the comand line
        self._getConfigData(arguments)
        
    def _getArguments(self):
        #This function parses the command line arguments
        parser = argparse.ArgumentParser(description='Code used to process calibrations')

        #Sets flag for the configuration file location
        parser.add_argument('-config', action='store', help='Configuration file to parse', type = str, required = True)
        #Sets flag for the configuration file location (Optional)
        parser.add_argument('-caltype', action='store', default='Sine,Step,Random', help='(Optional) Type of calibration to compute. Choose from {sine, step, random} separated by commas.', type = str, required = False)
        
        #Manual override for  sensor type
        parser.add_argument('-sentype', action='store', default=None, help='(Optional) Manual override for sensor type.', type = str, required = False)
        #Manual override for start date & time
        parser.add_argument('-startdate', action='store', default=None, help="(Optional) Manual override for start time. Format as 'YYYY-MM-DD hh:mm:ss'.", type = str, required = False)
        #Manual override for end date & time
        parser.add_argument('-duration', action='store', default='0', help="(Optional) Manual override for calibration duration in seconds.", type = str, required = False)
        #Manual override for input data location
        parser.add_argument('-inputloc', action='store', default=None, help="(Optional) Manual override for inumpyut data location.", type = str, required = False)
                #Manual override for inumpyut data location
        parser.add_argument('-outputloc', action='store', default=None, help="(Optional) Manual override for output data location.", type = str, required = False)
        #Manual override for  capacitive vs. resistive calibration type
        parser.add_argument('-cr', action='store', default='R', help="(Optional) Manual override for capacitive vs. resistive random calibration. {C = capacitive, R = resistive}'.", type = str, required = False)
        
        parserval = parser.parse_args()
        return parserval
    
    def _getConfigData(self, arguments):
        with open(arguments.config, 'r') as stream:
            conf = yaml.load(stream)
            self.dbname = conf['database']
            self.host = conf['host']
            self.username = conf['username']
            self.password = conf['password']
        self.calibrationType = arguments.caltype.lower()
        self.sentype = arguments.sentype
        self.startdate = arguments.startdate
        self.duration = int(arguments.duration)* 1000 #convert from seconds to milliseconds
        self.inputloc = arguments.inputloc
        self.outputloc = arguments.outputloc
        self.cr = arguments.cr
        