# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 12:31:16 2019

@author: klosmarc
"""


import sys, getopt
import m_sport as sp
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

import db2access as db

import m_logger as log
import m_utils as ut
import os
import addactivity




tcx2csv_xlst_filename="tcx2csv.xsl"



def tcx_sum(tcx_string, profiles_txt, user, context=None):
        p=sp.SportProfiles(text=profiles_txt)        
        if context: p.set_context(context)
        csv_string=str(sp.tcx2csv_from_str(tcx_string, tcx2csv_xlst_filename))
        outf=open("csv_output","w")
        outf.write(csv_string)
        outf.close()
        sum=sp.SportSummarize(csvstring=csv_string, profiles=p, parent=p)           
        
        
        sum.execute(user)
        return sum        
        



def plan_upload(plan_text, _override, _version, user_name, password, context):
         
        dbobj=sp.dbAccess()    
        dbobj.set_context(context)
        if not dbobj.authent_user(user_name, password): return False
        
        profiles_txt=dbobj.db_get_profiles(user_name)
        
    
        
        if profiles_txt:
            
            
            res=sp.plan2csv(plan_text, profiles_txt, context, act_id_suffix=' manual '+_version)
            csv_string=res['csv']
            imTSS=res['imTSS']
            print csv_string
            print 'imTSS',imTSS
            outf=open("plan_csv_output","w")
            outf.write(csv_string)
            outf.close()
            p=sp.SportProfiles(text=profiles_txt, parent=dbobj)                                              
            summ=sp.SportSummarize(csvstring=csv_string, profiles=p, parent=p)    
            summ.execute(user_name, delta_time_reset=False)         
            summ.persist(user_name, _version, override=_override)
            
            #print csv_string
            #print summ.summ
   #         sum=tcx_sum(tcx_string, profiles_txt, user_name, context)      
#          
  #          sum.persist(user_name, version, override=_override)
  
  
            for item in imTSS:                
                addactivity.addactivity(item['datetime'], item['discipline'], item['duration'], _version, _override, user_name,password,context, rTSS=item['rTSS'], hrTSS=item['hrTSS'], TSS=item['TSS'], sTSS=item['sTSS'] )

      
        else:
            msg="Profiles for user "+user_name+" not found id db"
            context.funlog().logger.error(msg)
            raise Exception(msg), None,  sys.exc_info()[2]

        
 


 
        
        


def main(argv):
    log.init("plan_upload")  
    context=ut.mContext()
    
    try:
        
        
    
    
        user=""
        password=""
        override=False;
        planfile=""
        version=""
         
        try:
            opts, args = getopt.getopt(argv,"f:u:p:ov:",["planfilename=","user=","password=", "override", "version="])         
        except getopt.GetoptError:
                print 'planupload.py -o -t <planfilename> -v <version> -u <user> -p <password>'
                sys.exit(2)
                

        for opt, arg in opts:
            if opt=="-f":
                planfile=arg 
            elif opt=="-u":
                    user=arg;
            elif opt=="-p":
                    password=arg;
            elif opt=="-o":
                    override=True;
            elif opt=="-v":
                    version=arg;

         
                          
        print "db connecting..."
        db.init(context)   
        print "db connected..."
                          
 
 
        planf=open(planfile,"r")
        plan_text=planf.read()            
        plan_upload(plan_text, override, version ,user,password, context)
        planf.close()
     
             
    except:     
        ut.proc_finally_exc(context)                   
    finally:
        ut.proc_finally(context)     
        db.close()

if __name__ == "__main__":
   main(sys.argv[1:])