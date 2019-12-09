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
        



def tcx_upload(tcx_string, _override, version, user_name, password, context):
         
        dbobj=sp.dbAccess()    
        dbobj.set_context(context)
        if not dbobj.authent_user(user_name, password): return False
        
        profiles_txt=dbobj.db_get_profiles(user_name)
        

        
        if profiles_txt:
            sum=tcx_sum(tcx_string, profiles_txt, user_name, context)      
            #print sum.get_summarize()
            sum.persist(user_name, version, override=_override)
        else:
            context.funlog().logger.error("Profiles for user "+user_name+" not found id db")

        
 

def main2(argv):
    
    try:
    
        
        db.init()      
        log.init("tcx_upload")
        
         

        tcxfile="Move_2019_10_08_14_24_38_Bieganie.tcx"
        tcxfile="Move_2019_10_14_16_17_57_Kolarstwo.tcx"
        tcxfile="Move_2019_11_10_12_29_50_Sport+multidyscyplinarny.tcx"
        tcxfile="Move_2019_10_14_16_17_57_Kolarstwo.tcx"
        tcxfile="Move_2019_11_10_12_29_50_Sport+multidyscyplinarny.tcx"
#        tcxfile="Move_2019_11_12_15_30_36_Bieganie.tcx"
        
        tcxf=open(tcxfile,"r")
        tcx_string=tcxf.read();                
        tcx_upload(tcx_string, True ,"marcin.klos.robert@gmail.com","test")
        
        tcxf.close()
         
    except Exception as e:  
            ut.proc_finally_exc()
                  
    finally:
        ut.proc_finally()
 
        
        


def main(argv):
    log.init("tcx_upload")  
    context=ut.mContext()
    
    try:
        
        
    
        tcxfile="Move_2019_10_08_14_24_38_Bieganie.tcx"
        tcxfile="Move_2019_10_14_16_17_57_Kolarstwo.tcx"
        tcxfile="Move_2019_11_10_12_29_50_Sport+multidyscyplinarny.tcx"
        user=""
        password=""
        override=False;
        tcxfile=None
        version="log"
         
        try:
            opts, args = getopt.getopt(argv,"t:u:p:ov:",["tcxfile=","user=","password=", "override", "version="])         
        except getopt.GetoptError:
                print 'tcxupload.py -o -t <inputtcxfile> -v <version> -u <user> -p <password>'
                sys.exit(2)
                

        for opt, arg in opts:
            if opt=="-t":
                tcxfile=arg 
            elif opt=="-u":
                    user=arg;
            elif opt=="-p":
                    password=arg;
            elif opt=="-o":
                    override=True;
            elif opt=="-v":
                    version=arg;

         
        tcx_list_path=[]

        if not tcxfile:
            cw=os.getcwd()
            tcx_list=os.listdir(cw+"/"+user+"/to_upload")
            for i,item in enumerate(tcx_list,0):         
                tcx_list_path.append(cw+"/"+user+"/to_upload/"+tcx_list[i])
            if not tcx_list:
                context.funlog().logger.info("no files to upload")
                return 0
        else: tcx_list_path=[tcxfile]                                

        db.init(context)      
                          
   #     print tcx_list_path

                
                
 #       tcxfile="Move_2019_10_08_14_24_38_Bieganie.tcx"
 #       tcxfile="Move_2019_10_14_16_17_57_Kolarstwo.tcx"
 #       tcxfile="Move_2019_11_10_12_29_50_Sport+multidyscyplinarny.tcx"
 #       tcxfile="Move_2019_10_14_16_17_57_Kolarstwo.tcx"
 #       tcxfile="Move_2019_11_10_12_29_50_Sport+multidyscyplinarny.tcx"
#        tcxfile="Move_2019_11_12_15_30_36_Bieganie.tcx"
               
        for i,tcxitem in enumerate(tcx_list_path,0):
            print "processed: "+tcx_list[i]
            tcxf=open(tcxitem,"r")
            tcx_string=tcxf.read();                
            tcx_upload(tcx_string, override, version ,user,password, context)
            tcxf.close()
            if not tcxfile:
                src=os.getcwd()+"\\"+user+"\\to_upload\\"+tcx_list[i]
                dst=os.getcwd()+"\\"+user+"\\uploaded\\"+tcx_list[i] 
    #            print src, dst
  #              os.rename(src, dst)
             
    except:     
        ut.proc_finally_exc(context)                   
    finally:
        ut.proc_finally(context)     
        db.close()

if __name__ == "__main__":
   main(sys.argv[1:])