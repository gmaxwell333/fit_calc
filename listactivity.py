# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 18:21:40 2019

@author: klosmarc
"""

import sys, getopt
import m_sport as sp



import db2access as db

import m_logger as log
import m_utils as ut

def listactivity(version,user, password,context):
    dbobj=sp.dbAccess()             
    dbobj.set_context(context)
    if not dbobj.authent_user(user, password): return False
    return dbobj.db_list_activities(version, user)       
            
def main(argv):       
    log.init("listactivity")      
    context=ut.mContext()      
    
    try:      
        user=""
        password=""
        version="log"
                  
        try:
            opts, args = getopt.getopt(argv,"u:p:v:",["user=","password=", "version="])
        except getopt.GetoptError:
                print 'listactivity.py -version <version> -u <user> -p <password>'
                sys.exit(2)
                    
        for opt, arg in opts:
            if opt=="-u":
                user=arg
            elif opt=="-p":
                password=arg;   
            elif opt=="-v":
                version=arg;   
    
           
        log.init("listactivity")
        db.init(context)                             
        res=listactivity(version,user,password, context)
        print("%-25s %-16s %-16s %-s " % ('start date_time', 'discipline', 'version', 'id'))
        for row in res:
            print("%-25s %-16s %-16s %-s " % (row['START_DATE_TIME'], row['DISCIPLINE_NAME'].strip(), row['VERSION'].strip(), row['EXTERNAL_ID'].strip()))
     
         
    except:     
        ut.proc_finally_exc(context)                   
    finally:
        ut.proc_finally(context) 
        db.close()

    
if __name__ == "__main__":
   main(sys.argv[1:])