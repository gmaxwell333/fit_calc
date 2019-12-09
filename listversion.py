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

def listversion(user, password,context):
    dbobj=sp.dbAccess()             
    dbobj.set_context(context)
    if not dbobj.authent_user(user, password): return False
    
    
    return dbobj.db_list_versions(user)       
            
def main(argv):       
    log.init("listversion")      
    context=ut.mContext()      
    
    try:      
        user=""
        password=""
                    
        try:
            opts, args = getopt.getopt(argv,"u:p:",["user=","password="])
        except getopt.GetoptError:
                print 'listversion.py -u <user> -p <password>'
                sys.exit(2)
                    
        for opt, arg in opts:
            if opt=="-u":
                user=arg
            elif opt=="-p":
                password=arg;   
           
           
        log.init("listversion")
        db.init(context)                             
        res=listversion(user,password, context)
        print("%-25s " % ('version'))
        for row in res:
            print("%-25s" % (row['VERSION']))
     
         
    except:     
        ut.proc_finally_exc(context)                   
    finally:
        ut.proc_finally(context) 
        db.close()

    
if __name__ == "__main__":
   main(sys.argv[1:])