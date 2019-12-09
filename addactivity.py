# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 18:19:10 2019

@author: klosmarc
"""


import sys, getopt
import m_sport as sp
import m_logger as log
import db2access as db    
import m_utils as ut
from collections import defaultdict

def addactivity(date_time, discipline, duration, version, override, user,password,_context, rTSS=None, hrTSS=None, TSS=None, sTSS=None ):

    dbobj=sp.dbAccess()             
    dbobj.set_context(_context)
    if not dbobj.authent_user(user, password): return False

        
    summ=sp.SportSummarize(context=_context)
   
    
    ext_id=date_time+" "+discipline
    summ.summ[ext_id]=defaultdict(lambda :0)
    summ.summ[ext_id]={"discipline":discipline, "duration":duration, "start_date_time": ut.strYmdHMS2datetime(date_time)}
    

    
    if rTSS:
        summ.summ[ext_id]["rTSS"]=rTSS
        summ.summ[ext_id]["rTSScover"]=duration
    if hrTSS:
        summ.summ[ext_id]["hrTSS"]=hrTSS
        summ.summ[ext_id]["hrTSScover"]=duration   
    if TSS:
        summ.summ[ext_id]["TSS"]=TSS
        summ.summ[ext_id]["TSScover"]=duration
    if sTSS:
        summ.summ[ext_id]["sTSS"]=sTSS
        summ.summ[ext_id]["sTSScover"]=duration              
        
        
    summ.persist(user,version, override)
         
    
    print summ.summ
    
  

    
def main(argv):   
    
   log.init("addactivity")      
   context=ut.mContext()      
        
    
   try:    
        user=""
        password=""       
        date_time=None
        duration=None
        rTSS=None
        hrTSS=None
        TSS=None
        sTSS=None
        version=None
        discipline=None
        override=False
         
        try:
            opts, args = getopt.getopt(argv,"u:p:o",["datetime=","discipline=", "duration=" ,"rTSS=","hrTSS=", "TSS=", "sTSS=","user=","password=", "version="])
        except getopt.GetoptError:
                print 'addactivity.py -d "<datetime>" -m <mass> -u <user> -p <password>'
                sys.exit(2)       
        
 #   --datetime "2019-12-05 10:10:00" --discipline Running --duration 3600 --rTSS=50 --sTSS=30 --hrTSS=80 --TSS=100 --version plan1 -u marcin.klos.robert@gmail.com -p test
                   
        for opt, arg in opts:
            if opt=="--datetime":
                date_time=arg         
            elif opt=="--discipline":
                discipline=arg
            elif opt=="--duration":
                duration=arg                
            elif opt=="--rTSS":
                rTSS=arg
            elif opt=="--hrTSS":
                hrTSS=arg
            elif opt=="--TSS":
                TSS=arg
            elif opt=="--sTSS":
                sTSS=arg;    
            elif opt=="--version":
                version=arg;                       
            elif opt=="-u":
                    user=arg;
            elif opt=="-p":
                    password=arg;       
            elif opt=="-o":
                    override=True;                        
     
                    
        db.init(context)

           
        addactivity(date_time, discipline, duration, version, override, user,password,context, rTSS, hrTSS, TSS, sTSS )

   except:     
       ut.proc_finally_exc(context)                   
   finally:
       ut.proc_finally(context) 
       db.close()

    
if __name__ == "__main__":
   main(sys.argv[1:])