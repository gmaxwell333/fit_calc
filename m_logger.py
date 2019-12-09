# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 13:28:38 2019

@author: klosmarc
"""

# -*- coding: utf-8 -*-

import logging

import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO


syslog=None




class Logger:
    logger=None
    string=None
    fh=None
    
    def __init__(self, name="root_logger", filename="default_logger.log", level=logging.DEBUG):       
        self.logger=logging.getLogger(name)
        self.string=StringIO()
        ch=logging.StreamHandler(self.string)
        self.logger.addHandler(ch)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        if filename:
            self.fh = logging.FileHandler(filename)
            if self.fh:
                self.logger.addHandler(self.fh)
                self.fh.setFormatter(formatter)
        
        self.logger.setLevel(logging.DEBUG)

    def read(self):
       return self.string.getvalue()
   
    def close(self):
        if self.fh: self.fh.close()
        
    



class StringLogger:
    logger=None
    iostr=None;
    
    
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.iostr = StringIO()
        ch = logging.StreamHandler(self.iostr)
        ch.setLevel(logging.DEBUG)
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        
    def read(self, clear=False):
        text=self.iostr.getvalue()
        if clear:
            self.iostr = StringIO()
            ch = logging.StreamHandler(self.iostr)
            ch.setLevel(logging.DEBUG)
            #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            #ch.setFormatter(formatter)
            self.logger.addHandler(ch)
        return text
    
    def info(self,text):
            self.logger.info(text)
        
    def add_handler(self,ch):
        self.logger.addHandler(ch)
        


def init(name):
    global syslog    
    syslog=Logger(name, name+".log")
 
    
    
    
