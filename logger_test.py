# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 13:14:14 2019

@author: klosmarc
"""

import logging
import sys


if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO


### Create the logger
root_logger=logging.getLogger('root_logger')
rcs = StringIO()
rch = logging.StreamHandler(rcs)
root_logger.addHandler(rch)

logger = logging.getLogger('basic_logger2')
logger.setLevel(logging.DEBUG)

##Setup the console handler with a StringIO object
log_capture_string = StringIO()
ch = logging.StreamHandler(log_capture_string)
ch.setLevel(logging.DEBUG)

### Optionally add a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

### Add the console handler to the logger
logger.addHandler(ch)
logger.addHandler(root_loggger)


### Send log messages. 
logger.debug(u'debug message')
logger.info('info message')
logger.warn('warn message')
logger.error('error message')
logger.critical('critical message')
root_logger.critical('root message')


### Pull the contents back into a string and close the stream
log_contents = log_capture_string.getvalue()


### Output as lower case to prove it worked. 
print(log_contents.lower())

log_contents = rcs.getvalue()


### Output as lower case to prove it worked. 
print(log_contents.lower())