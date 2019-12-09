# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 18:24:56 2019

@author: klosmarc
"""
from datetime import date
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt



from dateutil.rrule import rrule, DAILY

a = date(2009, 5, 30)
b = date(2009, 6, 9)
c=datetime(2009,6,7,5,0,0,0)
if b==c.date(): print 1

for dt in rrule(DAILY, dtstart=a, until=b):
    print type(dt)
    print dt.strftime("%Y-%m-%d")
    
    
list=[2,3,7,8]


a=1
print list
for i,l in enumerate(list,0):
    print l
    if l!=a: list.insert(i,a)
    a+=1
    print list   



fig = plt.figure()
x = np.arange(10)
y = 2.5 * np.sin(x / 20 * np.pi)
yerr = np.linspace(0.05, 0.2, 10)

plt.errorbar(x, y + 3, yerr=yerr, label='both limits (default)')

fig = plt.figure()

plt.errorbar(x, y + 2, yerr=yerr, uplims=True, label='uplims=True')

plt.errorbar(x, y + 1, yerr=yerr, uplims=True, lolims=True,
             label='uplims=True, lolims=True')

upperlimits = [True, False] * 5
lowerlimits = [False, True] * 5
plt.errorbar(x, y, yerr=yerr, uplims=upperlimits, lolims=lowerlimits,
             label='subsets of uplims and lolims')

plt.legend(loc='lower right')