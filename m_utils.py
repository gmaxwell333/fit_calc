# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 11:58:04 2019

@author: klosmarc
"""

import datetime
import re
import math as m
import m_logger as log
import sys
import traceback
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO
    

def strYmd2datetime(d):
    return datetime.datetime.strptime(d, '%Y-%m-%d')

def strYmdHMS2datetime(d):
    return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')

def xsd_dateTime2datetime(d):
    return datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')

def hourmin2sec(hm):
    match=re.search("^([0-9]?[0-9]?[0-9]):([0-5][0-9])$", hm)    
    #match=re.search("^(0[0-9]|1[0-9]|2[0-3]|[0-9]):([0-5][0-9])$", hm)
    if match==None:
        raise Exception("Invalid hh:mm format: "+ hm)    
    return int(match.group(1))*60+int(match.group(2));

def hourHmin2sec(hm):
    match=re.search("^([0-9]?[0-9])h([0-5][0-9])$", hm)    
        #match=re.search("^(0[0-9]|1[0-9]|2[0-3]|[0-9]):([0-5][0-9])$", hm)
    if match==None:
        raise Exception("Invalid hh:mm format: "+ hm)    
    return int(match.group(1))*3600+int(match.group(2))*60;


def hourmin_km2sec_m(hm):
    
    return float(hourmin2sec(hm))/1000

def hourmin_100m2sec_m(hm):
    
    return float(hourmin2sec(hm))/100
    

def speed_ms2pace_seckm(speed):
        return 1/(speed/1000)
    
def speed_ms2pace_sec100m(speed):
        return 1/(speed/100)
    
def speed_ms2pace_minkm(speed):
    psk=speed_ms2pace_seckm(speed)
    pmk=float(psk)/60
    re=m.modf(pmk);
    minu=int(re[1])
    sec=int(re[0]*60)
    dt=datetime.datetime(1970,1,1,minute=minu, second=sec)
    return(dt.strftime("%M:%S"))
    
def interp(a, x,y):
  #  print x
  #  print y
    
 #   print x
 #   print y
    l=len(x)
    for i,v in enumerate(x,1):       
 #       print i,v,a
        if(v>a):           
            break
    #print i,v
    i=i-1;
    if i>0 and i<=l and x[i]>=a:
       
        x1=x[i-1]
        x2=x[i]
        y1=y[i-1]
        y2=y[i]
        dx=x2-x1
        dy=y2-y1
        da=a-x1
        w=float(dy)/float(dx)
        dya=w*da
        ya=y1+dya
  #      print  "in: a=",a,"x1=",x1,"x2=",x2,"y1=",y1,"y2=",y2,"dx=",dx,"dy=",dy,"da=",da,"w=",w,"dya=",dya,"ya=",ya

        
        
    if i==0:
       
        x1=x[0]
        x2=x[1]
        y1=y[0]
        y2=y[1]
        dx=x2-x1
        dy=y2-y1
        da=x1-a
        w=float(dy)/float(dx)
        dya=w*da
        ya=y1-dya       
#        print  "left: a=",a,"x1=",x1,"x2=",x2,"y1=",y1,"y2=",y2,"dx=",dx,"dy=",dy,"da=",da,"w=",w,"dya=",dya,"ya=",ya

   # print i,l-1,a,x[i]
        
    if i==l-1 and x[i]<=a:    
        x1=x[i-1]
        x2=x[i]
        y1=y[i-1]
        y2=y[i]
        dx=x2-x1
        dy=y2-y1
        da=a-x2
        w=float(dy)/float(dx)
        dya=w*da
        ya=y2+dya
 #       print  "right: a=",a,"x1=",x1,"x2=",x2,"y1=",y1,"y2=",y2,"dx=",dx,"dy=",dy,"da=",da,"w=",w,"dya=",dya,"ya=",ya

    return ya        
        
        
        
    
    
def proc_finally_exc(context):
          s=StringIO()  
          exc_type, exc_value, exc_traceback = sys.exc_info()
          traceback.print_exc(file=s)
 #         log.syslog.logger.error(s.getvalue())
 #         log.syslog.logger.error("error: see log")
          
          s=StringIO()  
          exc_type, exc_value, exc_traceback = sys.exc_info()
          traceback.print_exc(file=s)
          context.syslog().logger.error(s.getvalue())
   #       context.syslog().logger.error("error: see log")
       
def proc_finally(context):
#        print log.syslog.read()
#        print log.funlog.read()
        print(context.syslog().read())
        print(context.funlog().read())
        log.syslog.close()        

class mContext:
    loggers=None    
     
    def __init__(self):
        self.create_def_loggers() 
    
    def set_loggers(self,loggers):
        self.loggers=loggers        
    
    def create_def_loggers(self):    
        syslog=log.syslog
        if not syslog: syslog=log.StringLogger("default_syslogger")
        usrlog=log.StringLogger("userlog")
        funlog=log.StringLogger("funlog")
        funlog.add_handler(usrlog.logger)
        usrlog.add_handler(syslog.logger)
        
        self.loggers={"userlog" : usrlog, "funlog" : funlog, "syslog": syslog}
        
        
    def syslog(self):
        return self.loggers["syslog"]
    
    def funlog(self):
        return self.loggers["funlog"]  
    
    def usrlog(self):
        return self.loggers["usrlog"]
        
class mBase():
    context=None    
    
    def __init__(self, parent=None):
        if not parent:
            self.context=mContext()
        else: self.repcon(parent)            
        
        
    def set_context(self,context):
        self.context=context
        
    def get_context(self,context):
        return self.context
    
    def repcon(self, parrent):
        self.set_context(parrent.get_context(self))
    
    def syslog(self):
        return self.context.syslog()
    
    def funlog(self):
        return self.context.funlog()
    
    def usrlog(self):
        return self.context.usrlog()  
    
    
class cyclingBuffor():
    buffor=None
    start=None
    stop=None
    size=None
    
    def __init__(self, size):
        self.size=size
        self.reset()

 
        

    def add(self,value):
        
        if self.start>self.stop and self.stop>-1:           
            self.start=self.start+1
            if self.start==self.size:
                 self.start=0        

        self.stop=self.stop+1        
        if self.stop==self.size:
            self.stop=0
            self.start=self.start+1

        self.buffor[self.stop]=value
    
 #       print self.start
 #       print self.stop
 #       print self.buffor
    
    def get(self,n):
        if self.stop==-1: return None
        if n>self.count()-1: return None
        i=self.start+n
        if i>=self.size: i=i-self.size
        
        return self.buffor[i]

    def summ(self):
        if self.stop==-1: return None
        s=0    
        if self.stop>=self.start:
            for i in range(self.start,self.stop+1):
       #         print i
                s=s+self.buffor[i]
        else:                       
            for i in range(self.start,self.size):
     #           print "s1",i
                s=s+self.buffor[i]
            for i in range(0,self.stop+1):
      #          print "s2",i    
                s=s+self.buffor[i]           
        return s           
    
    def count(self):
        if self.stop==-1: return 0
        c=self.stop-self.start
        if c>=0: 
            return c+1
        else: return self.size
    
    def avg(self, full=False):        
        if self.stop==-1: return None
        c=self.count()
        if full and c<self.size: return None
        if c>0:
            return float(self.summ())/self.count()
        else: return None
        
    def reset(self):
        self.start=0
        self.stop=-1            
        self.buffor=[0]*self.size
        
                    
        
    
#def strYmdHMS2datetime(str):
#    return datetime.datetime.strptime(str, '%Y-%m-%d %H %M %S')