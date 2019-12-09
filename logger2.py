# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 13:28:38 2019

@author: klosmarc
"""

# -*- coding: utf-8 -*-

import logging

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO


### Create the logger
logger = logging.getLogger('basic_logger4')
logger.setLevel(logging.DEBUG)

##Setup the console handler with a StringIO object
log_capture_string = StringIO()

#print >>log_capture_string, "Line"
ch = logging.StreamHandler(log_capture_string)
ch.setLevel(logging.DEBUG)

### Optionally add a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

### Add the console handler to the logger
logger.addHandler(ch)


### Send log messages. 
logger.debug(u'debug message1')
#logger.info('info message')
#logger.warn('warn message')
#logger.error('error message')
#logger.critical('critical message')


### Pull the contents back into a string and close the stream
log_contents = log_capture_string.getvalue()
print(log_contents.lower())
log_capture_string.flush()
log_contents = log_capture_string.getvalue()


### Output as lower case to prove it worked. 
print(log_contents.lower())
logger.debug(u'debug message2')
log_contents = log_capture_string.getvalue()
print(log_contents.lower())



