# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 15:37:59 2019

@author: klosmarc
"""


import m_sport as sp
import m_utils as ut

m=sp.runPowerModel_1() 

# m.get_power(75,1150,cad=180,step_h=0.1, step_factor=0.5)

#print m.get_power(60,500, cad=180, step_h=0.1, step_factor=0.2)
#print m.get_norm_pace(60,6*60, 0.19,cad=180,step_h=0.1, step_factor=0.5)/60
#print m.get_norm_pace(75,4.5*60, 1,cad=180,step_h=0.1, step_factor=0.5)/60


b=ut.cyclingBuffor(5)

b.add(1)      
b.add(2) 
b.add(3) 
b.add(4) 
b.add(5) 
b.add(6) 
b.add(7) 
b.add(8) 
b.add(9) 
b.add(10) 
b.add(11)

b.reset()


print "get ",b.get(0)
print "summt ",b.summ()
print "count ",b.count()
print "avg ",b.avg()
cw=os.getcwd()

print os.listdir(cw+"/to_upload")
                  
 