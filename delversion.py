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

def delversion(version, user,password, context):
    dbobj=sp.dbAccess()             
    dbobj.set_context(context)
    if not dbobj.authent_user(user, password): return False
    
   
    del_rec=dbobj.db_del_version(user, version)
    if del_rec!=None:
        if del_rec>0:
            context.funlog().logger.info("The version "+ version+ " was deleted, "+str(del_rec)+" activities")
        else: 
                context.funlog().logger.info("The version "+ version+ " was NOT deleted, 0 activities found")
    else: context.funlog().logger.info("delete sql error")                
   

    
def main(argv):   
    
    log.init("delversion")      
    context=ut.mContext()      
   
    try:    
        user=""
        password=""
        version=""               
        try:
            opts, args = getopt.getopt(argv,"v:u:p:",["version=","user=","password="])
        except getopt.GetoptError:
                print 'delversion.py -i <id> -u <user> -p <password>'
                sys.exit(2)                
        for opt, arg in opts:
            if opt=="-v":
                version=arg         
            elif opt=="-u":
                    user=arg;
            elif opt=="-p":
                    password=arg;

        log.init("delversion")
        db.init(context)
        delversion(version, user,password, context)              
    except:     
       ut.proc_finally_exc(context)                   
    finally:
       ut.proc_finally(context)
       db.close()

    
if __name__ == "__main__":
   main(sys.argv[1:])