# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 11:50:13 2019

@author: klosmarc
"""


import sys, getopt
import m_sport as sp
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

import m_logger as log
import db2access as db
import m_utils as ut


def profiles_upload(profiles_string, user_name, password, context):

    dbobj=sp.dbAccess()             
    dbobj.set_context(context)
    if not dbobj.authent_user(user_name, password): return False
    
    p=sp.SportProfiles(text=profiles_string)   
    
    if context: p.set_context(context)

    try:
        if not dbobj.db_persist_profiles(profiles_string, user_name):
            context.funlog().logger.error("Profile upload error, user name "+user_name)
        else:
            context.funlog().logger.error("Profile uploaded, user name "+user_name)
    except Exception as e:
        context.syslog().logger.error(e.message)
        context.funlog().logger.error("Profile upload error, user name "+user_name)
        raise e, None, sys.exc_info()[2]
    


def main(argv):
    log.init("profupload")    
    context=ut.mContext()    
    
    try:
            
        profiles_file=""
        user=""
        password=""
        
   
         
        try:
            opts, args = getopt.getopt(argv,"i:u:p:",["profilesfile=","user=","password="])
        except getopt.GetoptError:
                print 'profupload.py -i <inputprofilesfile> -u <user> -p <password>'
                sys.exit(2)
                    
        for opt, arg in opts:
            if opt=="-i":
                profiles_file=arg
          
            elif opt=="-u":
                    user=arg;
            elif opt=="-p":
                    password=arg;
          
        db.init(context)
                     
        
        pf=open(profiles_file,"r")
        pf_string=pf.read();   
             
        profiles_upload(pf_string, user,password, context)
        pf.close()
         
    except:     
        ut.proc_finally_exc(context)                   
    finally:
        ut.proc_finally(context)     
        db.close()

    
if __name__ == "__main__":
   main(sys.argv[1:])