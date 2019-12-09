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

def delactivity(ext_id, user,password, context):
    dbobj=sp.dbAccess()             
    dbobj.set_context(context)
    if not dbobj.authent_user(user, password): return False

    if dbobj.db_get_activity(user, ext_id): 
            if dbobj.db_del_activity(user, ext_id):           
                context.funlog().logger.info("The activity "+ext_id +" deleted")
            else: 
                context.funlog().logger.info("The activity "+ext_id +" NOT deleted")
    else:
        context.funlog().logger.info("The activity "+ext_id +" NOT found")


    
def main(argv):   
    
    log.init("delactivity")      
    context=ut.mContext()      
   
    try:    
        user=""
        password=""
        ext_id=""               
        try:
            opts, args = getopt.getopt(argv,"i:u:p:",["id=","user=","password="])
        except getopt.GetoptError:
                print 'delactivity.py -i <id> -u <user> -p <password>'
                sys.exit(2)                
        for opt, arg in opts:
            if opt=="-i":
                ext_id=arg         
            elif opt=="-u":
                    user=arg;
            elif opt=="-p":
                    password=arg;

        log.init("delactivity")
        db.init(context)
        delactivity(ext_id, user,password, context)              
    except:     
       ut.proc_finally_exc(context)                   
    finally:
       ut.proc_finally(context)
       db.close()

    
if __name__ == "__main__":
   main(sys.argv[1:])