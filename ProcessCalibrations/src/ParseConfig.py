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
        parser.add_argument('-config', action = "store", help="Configuration file to parse", type = str, required = True)
        #Sets flag for the configuration file location
        parser.add_argument('-caltype', action = "store", default="Sine,Step,Random", help="Type of calibration to compute. Choose from {sine, step, random} separated by commas.", type = str, required = False)
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