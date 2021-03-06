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


def setpersonal(date_time, data, user,password,context):
    
    dbobj=sp.dbAccess()             
    dbobj.set_context(context)
    if not dbobj.authent_user(user, password): return False

    if dbobj.db_update_personal(date_time, data, user):         
  #  if sp.db_insert_personal(date_time, data, user):        
        context.funlog().logger.info("The personal data SET: "+date_time +" "+user+", " + str(data))
    else: 
        context.funlog().logger.info("The personal data "+date_time +" "+user+" NOT set")   
#    db.close()

    
def main(argv):   
   result=0 
#   print('starting...')
   log.init("setpersonal")      
   context=ut.mContext()      
          
    
   try:    
        user=""
        password=""
        mass=None    
        date_time=None
         
        try:
            opts, args = getopt.getopt(argv,"d:m:u:p:",["datetime=","mass=","user=","password="])
        except getopt.GetoptError:
                print('setpersonal.py -d "<datetime>" -m <mass> -u <user> -p <password>')
                sys.exit(2)       
        
    
                   
        for opt, arg in opts:
            if opt=="-d":
                date_time=arg         
            elif opt=="-m":
                mass=arg;
            elif opt=="-u":
                    user=arg;
            elif opt=="-p":
                    password=arg;                    
     
    #    print('db connecting...')                    
        db.init(context)
    #    print('db connected')
        

        data={"mass" : mass}
    
        setpersonal(date_time, data, user,password,context)              
   except:     
       ut.proc_finally_exc(context)           
       result=-1;         
   finally:
       ut.proc_finally(context) 
       db.close()
       
       
   return result         
    
if __name__ == "__main__":

   result=main(sys.argv[1:])
   
   sys.exit(result)
