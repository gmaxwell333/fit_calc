# -*- coding: utf-8 -*-
"""
#Created on Mon Nov  4 20:23:06 2019

@author: klosmarc
"""


import lxml.etree as ET
from json import loads
import m_utils as ut
from collections import defaultdict
import math as m
import db2access as db
import ibm_db
import m_logger as log
import re
import datetime as dt




import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO


import pandas as pd
import m_logger as ml


def tcx2csv(tcxfile,xslfile):
    tcxdom = ET.parse(tcxfile)
    xslt = ET.parse(xslfile)
    transform = ET.XSLT(xslt)
    csv = transform(tcxdom)
    return csv



def tcx2csv_from_str(tcx_string, xslfile):
    tcx_stream=StringIO(tcx_string)
    tcxdom = ET.parse(tcx_stream)
    xslt = ET.parse(xslfile)
    transform = ET.XSLT(xslt)
    csv = transform(tcxdom)
    return csv


"""
def plan2csv

    segment wariants:
        Running: pace/time > speed/time
                 zone(p)/time > speed/time 
                 pace/dist > speed/time
                 zone(p)/dist > speed/time
    
        Biking:  zone(hr)/time > hr/time
                # power
                # zone(power)
        
        Swimming: pace(100m)/time > speed/time
                  pace(100m)/dist > speed/time
                  zone(p100m/time) > speed/time
                  zone(p100m)/dist > speed/time
    
    total wariants:
         Running: distance or duration
         Biking: duration
         Swimming: distance or duration
    
    return: csv, None : error       
        
"""



def plan2csv(plan_text, profiles_txt, context, act_id_suffix=""): 
    csv=""
    prof=SportProfiles(text=profiles_txt)        
    if context: prof.set_context(context)    
    
    
    regexhead = r"(?P<head>(.*\s+)?(?P<date>\d\d\d\d-(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[1|2][0-9]|3[0-1]))\s+(?P<time>(?:0?[0-9]|00|[0-9]|1[0-9]|2[0-3]):(?:[0-9]|[0-5][0-9]):(?:[0-9]|[0-5][0-9]))\s+(?P<discipline>\w+)\s+((?:(?P<total_distance>(?:[0-9])+)(?P<total_distance_unit>m|km))|(?P<total_duration>(?:0?[0-9]|00|[0-9][0-9])h(?:[0-5][0-9]))))\s+(?:(?:TSS=(?P<TSS>[0-9]+))|(?:rTSS=(?P<rTSS>[0-9]+))|(?:hrTSS=(?P<hrTSS>[0-9]+))|(?:sTSS=(?P<sTSS>[0-9]+)))?(.*|(.*#35*))$"
   # regexhead = r"(?P<head>(?P<date>\d\d\d\d-(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[12][0-9]|3[01])) (?P<time>(?:0?[0-9]|00|[0-9]|1[0-9]|2[0-3]):(?:[0-9]|[0-5][0-9]):(?:[0-9]|[0-5][0-9]))\s+(?P<discipline>\w+)\s+(?:(?:(?P<total_distance>(?:[0-9])+)m)|(?P<total_duration>(?:0?[0-9]|00|[0-9][0-9])h(?:[0-5][0-9]))))\s+(?:(?:TSS=(?P<TSS>[0-9]+))|(?:rTSS=(?P<rTSS>[0-9]+))|(?:hrTSS=(?P<hrTSS>[0-9]+))|(?:sTSS=(?P<sTSS>[0-9]+)))?.*$"
    regexseg  = r"^\s*(?P<segment>(?:(?P<segpace>(?:(?:0?[0-9]|00|[0-9]|[0-9][0-9]):(?:[0-5][0-9])))|(?P<zone>z[0-9]))\/(?:(?P<segtime>(?:(?:[0-9]?)[0-9]?(?:[0-9]:)?)(?:[0-9]?(?:[0-9]:))(?:[0-5][0-9]))|(?:(?:(?P<segdist>[0-9]+)(?P<segdistunit>m|km)))|(?:(?:(?P<segsec>[0-9]+)(?:s))))).*$";
  
    plan = StringIO(plan_text)
 
    csv="discipline,id,type,date_time,heartrate_bpm,speed,power,altitude_m,latitude,longitude,distance\n"    
    imTSS=[]
    
    for line in plan:
        base_line=line
#        print line.replace("\n", "")
        if "off" in line: continue
        line=line.strip()
        if line:  
            if line[0]=="#": continue
            total_segtime=0
            total_segdist=0
            match = re.match(regexhead, line)                                         
            
            if match:  
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1        
        #            print ("Group {groupNum} found at {start}-{end}: {group}".format(groupNum = groupNum, start = match.start(groupNum), end = match.end(groupNum), group = match.group(groupNum)))
            
                head=match.groupdict()
                date=head["date"]
                time=head["time"]
                datetime=date+" "+time
                xsd_datetime=(datetime+"Z").replace(' ', 'T')
                act_id=xsd_datetime+act_id_suffix
                discipline=head["discipline"]
                duration=head["total_duration"]
                if head["total_distance"]!=None:
                    distance=float(head["total_distance"])
                    if head["total_distance_unit"]=='km': distance=distance*1000
                    
                else: distance=None                    
                duration_s=None
                if duration!=None:
                    duration_s=ut.hourHmin2sec(duration)
                tp_datetime=ut.strYmdHMS2datetime(datetime)
                
                prof.select_profile_by_date(date)        
                prof.select_discipline(discipline)
                
                if head['TSS'] or head['rTSS'] or head['hrTSS'] or head['sTSS']:
                    rec={}
                    rec['datetime']= head['date'] + ' ' + head['time']
                    rec['discipline']=head['discipline']
                    rec['duration']=duration_s
                    rec['TSS']=head['TSS']
                    rec['sTSS']=head['sTSS']
                    rec['hrTSS']=head['hrTSS']
                    rec['rTSS']=head['rTSS']
                    imTSS.append(rec)
                    
                else:
         #           print line
         #           print head["head"]
                    line=line.replace(head["head"], "")
                    line=line.strip()
          #          print line

                    csv=csv+'{_discipline},{_id},trackpoint,{_date_time},0,0,,,,\n'.format(_discipline=discipline,_id=act_id,_date_time=(str(tp_datetime)+'Z').replace(' ', 'T') )                                                                                     
              


                    while line:                        
          #              print line
                        if line[0]=="#": break
                        matchseg = re.match(regexseg, line)
                        seg=matchseg.groupdict()
             #           print line
  #                      print seg
                        line=line.replace(seg["segment"],"",1)
                        line=line.strip()
                        
                        zone=None
                        segspeed=None
                        
                        #designate speed, time, dist for Running or Swimming
                        if discipline=='Running' or discipline=='Swimming':
                            #designate pace                            
                            pace=None                            
                            if seg["segpace"]: 
                                pace=seg["segpace"]                              
                            else:
                                zone=seg["zone"]
      #                          print zone
                                pace=prof.get_pace_by_zone(zone)
                                if not pace: 
                                    print("no pace for zone "+zone+" found in profile: "+ prof.profile_date +"/"+discipline)
                                    return None
                          # print pace                      
                            
                            if discipline=='Running': 
                                segspeed=1/ut.hourmin_km2sec_m(pace)
                            if discipline=='Swimming':                             
                                print(pace)
                                segspeed=1/ut.hourmin_100m2sec_m(pace)
#                        
                            
                            
                            #designate segtime and segdist
                            segdist=None
                            segtime=None
                            if seg["segsec"]: 
                                segtime=float(seg["segsec"])
                            elif seg["segtime"]: 
                                segtime=ut.hourmin2sec(seg["segtime"])  
                            elif seg["segdist"]:
                                segdist=float(seg["segdist"])
                                if seg["segdistunit"]=='km': segdist=segdist*1000      
                                segtime=float(segdist)/segspeed 

                            if segdist==None: segdist=segspeed*segtime   

                            total_segtime=total_segtime+segtime    
                            total_segdist=total_segdist+segdist                                  
                            
                        
         
                                
                        #designate hr, time for Biking  
                        hr=None
                        if discipline=='Biking': 
                            
                            #designate hr
                            if seg["zone"]:        
                                hr=prof.get_hr_by_zone(seg["zone"])
                                if not hr:
                                    msg="no pace for zone "+zone+" found in profile: "+ prof.profile_date +"/"+discipline+", "+base_line
                                    context.funlog().logger.error(msg)
                                    raise Exception(msg), None,  sys.exc_info()[2]
                            elif seg["pace"]:
                                msg="segment pace definition not allowed for, only zone/time allowed, "+base_line
                                context.funlog().logger.error(msg)
                                raise Exception(msg), None,  sys.exc_info()[2]
                            
                            #desingate segtime
                            segtime=None
                            if seg["segsec"]: 
                                segtime=float(seg["segsec"])
                            elif seg["segtime"]: 
                                segtime=ut.hourmin2sec(seg["segtime"])                             
                            elif seg["segdist"]: 
                                msg="segment dist definition not allowed, only zone/time allowed, "+base_line
                                context.funlog().logger.error(msg)
                                raise Exception(msg), None,  sys.exc_info()[2]
                            

                            if hr==None or segtime==None:
                                msg="segment zone/time definition not found "+base_line
                                context.funlog().logger.error(msg)
                                raise Exception(msg), None,  sys.exc_info()[2]
                    
                            total_segtime=total_segtime+segtime                            
                                                                                                             
                        
                        
           #             print "segspeed ",segspeed
           #             print "segdist ", segdist        
           #             print "segtime ", segtime       
           #             print "hr", hr
           #             print ""

                        hr_text=""
                        if hr!=None: hr_text=hr
                       
                        speed_text=""
                        if segspeed!=None: speed_text=segspeed    

                       
                        delta_time=dt.timedelta(seconds=round(segtime))
                        tp_datetime=tp_datetime+delta_time
                        
                        csv=csv+'{_discipline},{_id},trackpoint,{_date_time},{_heartrate_bpm},{_speed},,,,,\n'.format(_discipline=discipline,_id=act_id,_date_time=(str(tp_datetime)+'Z').replace(' ', 'T'),_heartrate_bpm=hr_text, _speed=speed_text )                                                                                                     
                
                    cpace=None
                    cspeed=None
                    cduration=None
                    cdistance=None
                    c_hr=None
                    
                    if discipline=='Running' or discipline=='Swimming':
                        cpace=prof.get_pace_by_zone('z2')
                        if discipline=='Running': 
                            cspeed=(1/ut.hourmin_km2sec_m(cpace))
                        if discipline=='Swimming':                             
#                            print cpace
                            cspeed=1/ut.hourmin_100m2sec_m(cpace)                            
                            
                    if discipline=='Biking':                            
                        c_hr=prof.get_hr_by_zone('z2')
    
    
                    
                    if distance!=None:
                        if discipline!='Running' and discipline!='Swimming':
                            msg="total duration not allowed for "+discipline+", "+base_line
                            context.funlog().logger.error(msg)
                            raise Exception(msg), None,  sys.exc_info()[2]
                            
                        cdistance=distance-total_segdist    
                        if cdistance<0: cdistance=0
                 
                    if duration_s!=None:
                        cduration=duration_s-total_segtime
                        if cduration<0: cduration=0                        
                    else: cduration=cdistance/cspeed                    
              
                
                    if cduration>0:            
                        hr_text=""
                        if hr!=None: hr_text=c_hr
                                       
                        speed_text=""
                        if segspeed!=None: speed_text=cspeed    
                                
                        delta_time=dt.timedelta(seconds=round(cduration))
                        tp_datetime=tp_datetime+delta_time                                
                               
                        csv=csv+'{_discipline},{_id},trackpoint,{_date_time},{_heartrate_bpm},{_speed},,,,,\n'.format(_discipline=discipline,_id=act_id,_date_time=(str(tp_datetime)+'Z').replace(' ', 'T'),_heartrate_bpm=hr_text, _speed=speed_text )                                                                                     
                                                                     
              #      print "total_segtime: ",total_segtime
              #      print "total_segdist:", total_segdist
              #      print "total_duration: ", duration_s
              #      print "total_distance: ", distance
              #      print "cspeed: ", cspeed
              #      print "c_hr: ", c_hr
              #      print "cduration: ", cduration
              #      print "cdistance: ", cdistance
            else:
                print("avtivity description error: "+base_line)
         
 
    
    return {'csv': csv, 'imTSS': imTSS}

"""                    
                        
                        if seg["segsec"]: 
                            segtime=float(seg["segsec"])
                        elif seg["segtime"]: 
                            segtime=ut.hourmin2sec(seg["segtime"])                          
                        elif seg["segdist"]:
                            if zone and discipline!="Running":
                                msg="combination zone and distance allowed only for Runnuning, for others only zone/time allowed, "+base_line
                                context.funlog().logger.error(msg)
                                raise Exception(msg), None,  sys.exc_info()[2]
                            segdist=float(seg["segdist"])
                                                                          
                            segtime=float(segdist)/segspeed                                                                                                                                            
                        else:
                                msg="nor distance neither time defined for segment, "+base_line
                                context.funlog().logger.error(msg)
                                raise Exception(msg), None,  sys.exc_info()[2]

"""

class dbAccess(ut.mBase):
    
    lowdb=None
    
    def __init__(self,parent=None):
        ut.mBase.__init__(self, parent)
        self.lowdb=db.lowDBAccess(parent)


    def db_get_profiles(self,user_name):
        sql=('SELECT profiles FROM "USERS" WHERE email=\'{email}\';').format(email=user_name)
        res=self.lowdb.execute_sql(sql)
        if res: return ibm_db.fetch_tuple(res)[0]
        else : None
        
    def db_persist_profiles(self,profiles, user_name):
        #print str(profiles)
        #profiles=str(profiles).replace("'", "\\'")
        if self.db_get_users(user_name):
            sql=('UPDATE "USERS" SET profiles=\'{_profiles}\' WHERE email=\'{_email}\';').format(_profiles=profiles,_email=user_name);  
            res=self.lowdb.execute_sql(sql)
            if res: return res
            return None
        else:
           self.funlog().logger.error("User NOT found: "+ user_name)
           return False
    
    
    def db_list_activities(self,version, user_name):
        sql=('SELECT start_date_time, discipline_name, version, external_id FROM ACTIVITY as a, DISCIPLINE as d WHERE a.discipline_id=d.discipline_id AND a.version=\'{_version}\' AND user_id=(SELECT user_id FROM "USERS" WHERE email=\'{email}\');').format(email=user_name, _version=version)
        stmt = self.lowdb.execute_sql_stmt(sql)
        res=[]
        if stmt:         
            row = ibm_db.fetch_assoc(stmt)
            while ( row ):
                res.append(row)
                row = ibm_db.fetch_assoc(stmt)
        else: return None
        
        
        return res
    
    
    
    
    def db_list_versions(self, user_name):
        sql=('SELECT version FROM ACTIVITY as a WHERE user_id=(SELECT user_id FROM "USERS" WHERE email=\'{email}\') GROUP BY version;').format(email=user_name)
        stmt = self.lowdb.execute_sql_stmt(sql)
        res=[]
        if stmt:         
            row = ibm_db.fetch_assoc(stmt)
            while ( row ):
                res.append(row)
                row = ibm_db.fetch_assoc(stmt)
        else: return None
        
        return res
    
    def db_del_activity(self,user_name, external_id):                    
                    res=self.lowdb.execute_sql("DELETE FROM activity WHERE external_id=\'{_external_id}\' AND user_id=(SELECT user_id FROM USERS WHERE email=\'{_email}\');".format(_external_id=external_id, _email=user_name))            
                    if res :return True      
                    else: return False
    
    def db_del_version(self,user_name, version):       # return number of deleted rows  or None if error           
                    res=self.lowdb.execute_sql("DELETE FROM activity WHERE version=\'{_version}\' AND user_id=(SELECT user_id FROM USERS WHERE email=\'{_email}\');".format(_version=version, _email=user_name))            
                    if res : return self.lowdb.get_num_row_aff()
                    else: return None
                      
                
             
                
                
    def db_get_activity(self,user_name, external_id):
                res=self.lowdb.execute_sql("SELECT * FROM activity WHERE user_id=(SELECT user_id FROM USERS WHERE email=\'{_email}\') AND external_id=\'{_external_id}\';".format(_email=user_name, _external_id=external_id))    
                return ibm_db.fetch_tuple(res);            
            
            
    def db_update_personal(self,date_time2, data, user):             
        if self.db_list_personal(user, date_time=date_time2):
                sql_comm="UPDATE personal"
                sql_where="WHERE user_id=(SELECT user_id FROM USERS WHERE email=\'{_email}\') AND date_time=\'{_datetime}\'".format(_email=user, _datetime=date_time2)   
                sql_set="SET "            
                if data:
                    for key, value in data.items():  
                        sql_set=sql_set+key+"="+"\'"+value+"\',"
                    sql_set=sql_set[0:-1]
                    sql=sql_comm+" "+sql_set+" "+sql_where+";"
                    return self.lowdb.execute_sql_stmt(sql)                                
                else:
                    self.funlog().logger.error("No personal data provided")
                    return False
                return True
        else : return self.db_insert_personal(date_time2, data, user)
                    
 
                    
            
    def db_insert_personal(self,date_time, data, user):  
        if self.db_get_users(user):
                sql_comm="INSERT INTO personal"
                sql_fields="(USER_ID, date_time,"
                sql_values="VALUES((SELECT user_id FROM USERS WHERE email=\'{_email}\'), \'{_date_time}\',".format(_email=user,_date_time=date_time)
                
        
                if data:
                    for key, value in data.items():  
                        sql_fields=sql_fields+key+","
                        sql_values=sql_values+"\'"+value+"\',"
                    sql_fields=sql_fields[0:-1]+")"
                    sql_values=sql_values[0:-1]+")"
                    sql=sql_comm+" "+sql_fields+" "+sql_values+";"
        #            print sql
                    return self.lowdb.execute_sql_stmt(sql)                               
                else:
                    self.funlog().logger.error("No personal data provided")
                    return False
                return True
        else:
            return False
            
    def db_get_users(self, user=None):
        dt_cond=""
        if user:
            dt_cond=" WHERE email=\'{_email}\'".format(_email=user)
        sql=('SELECT * FROM users'+dt_cond+';')     
  #      print sql
        stmt = self.lowdb.execute_sql_stmt(sql)    
        res=[]
        if stmt:         
            row = ibm_db.fetch_assoc(stmt)
            while ( row ):
                res.append(row)
                row = ibm_db.fetch_assoc(stmt)
        else: 
            self.funlog().logger.error("User NOT found: "+ user)
            return None        
        return res                

    def db_get_mass(self, user, dt):     
        res=self.db_list_personal(user, date_time=dt, found="up")    
   #     print "db mass"
   #     print res
        if res: return res[0]["MASS"]
        else:
            res=self.db_list_personal(user, date_time=dt, found="down")
            if res: return res[0]["MASS"]
        return None

    def db_list_personal(self,user_name, date_time=None, found=None):
        
        dt_cond=""
        sign="="
        limit=""
        dire="DESC"
        if  found:
            limit="LIMIT 1"
            if found=="up": sign="<="
            if found=="down":
                sign=">="
                dire="ASC"
            
        
        #print user_name,found, sign, limit
        
        if date_time:
            dt_cond=" AND date_time"+sign+"\'{_datetime}\'".format(_datetime=date_time)            
        sql=('SELECT * FROM personal as p WHERE user_id=(SELECT user_id FROM "USERS" WHERE email=\'{email}\') {_dt_cond} ORDER BY date_time {_desc} {_limit};').format(email=user_name, _dt_cond=dt_cond, _desc=dire, _limit=limit)    
        #print sql
        stmt = self.lowdb.execute_sql_stmt(sql)    
        res=[]
        if stmt:         
            row = ibm_db.fetch_assoc(stmt)
            while ( row ):
                res.append(row)
                row = ibm_db.fetch_assoc(stmt)
        else: return None
        
        return res                
   
    def authent_user(self, user, password): # results: True: authent, False: not authent, None: error
        dt_cond=""
        if user:
            dt_cond=" WHERE email=\'{_email}\' AND password=\'{_password}\'".format(_email=user, _password=password)
        sql=('SELECT * FROM users'+dt_cond+';')     
  #      print sql
        stmt = self.lowdb.execute_sql_stmt(sql)           
        if stmt==None: return None
        row = ibm_db.fetch_assoc(stmt)
        while ( row ):        
            return True                
        self.funlog().logger.error("user not authenticated")
        return False
        
               
            



class SportProfiles(ut.mBase):
    

       
    def __init__(self, filename=None, text=None, parent=None):
        ut.mBase.__init__(self, parent)
        try:        
            self.profiles={}
            self.reset();     
            if filename:self.loadfromfile(filename)
            elif text: self.loadfromtxt(text)
            else: raise Exception("Nor filename, neither text defined ")                    
        except Exception as e:
            self.funlog().logger.error(e.message)
            self.funlog().logger.error("Profiles data corrupted")
            raise e, None, sys.exc_info()[2]
   

        
    def loadfromfile(self,filename):
        f=open(filename, "r")        
        self.set_profiles(loads(f.read()))
        f.close()
        return True;                
    
    def loadfromtxt(self,text):       
        self.set_profiles(loads(text))
        return True;                
    
    def set_profiles(self,zones_dict):  
        self.profiles=zones_dict;
   
    def get_profiles(self):
        return self.profiles;
        

    
    def get_profiles_dates(self):
        return self.profiles.keys()
    
    def select_profile_by_date(self,date):
         
         if self.activity_date == None: self.activity_date=date
         elif self.activity_date==date: return True
         else: self.activity_date=date                                             
        
         dates=self.get_profiles_dates()
         
         if not dates : 
             print("No profiles defined")
             return False

         dates.sort(key=ut.strYmd2datetime, reverse=True)
    
         for i,d in enumerate(dates):
             if(ut.strYmd2datetime(date)>=ut.strYmd2datetime(d)):
                 break
                
         self.profile_date=d;
         return True
             
         
    def select_discipline(self,discipline):
        profile=self.get_selected_profile()        
        if not discipline in profile:            
            raise Exception("error: Discipline "+discipline+" in profile "+self.profile_date+" not found")            
        self.discipline=discipline         
        return True
         
    def get_selected_profile(self):
        if self.profile_date==None: 
            print("No profile selected")
            return None
        return self.profiles[self.profile_date]
    
    
    def get_rtp(self):      
        pace=self.get_base("pace");
        if not pace: return None
        if not "rtp" in pace:
            raise Exception("rtp field not found in profile "+self.profile_date+"/"+self.discipline+"/pace")            
        return pace["rtp"]
    
    def get_stp(self):      
        pace=self.get_base("pace");
        if not pace: return None
        if not "stp" in pace:
            msg="stp field not found in profile "+self.profile_date+"/"+self.discipline+"/pace"
            self.funlog().logger.error(msg)
            raise Exception(msg), None,  sys.exc_info()[2]
        return pace["stp"]    
    
    def get_rtp_sec(self):        
        rtp=self.get_rtp();
        if(rtp==None): return None            
        return ut.hourmin2sec(rtp)   
    
    
    def get_hrt(self):
        hr=self.get_base("hr")
        if not hr: return None
        if not "hrt" in hr:
            raise Exception("hrt field not found in profile "+self.profile_date+"/"+self.discipline+"/hr")            
        return int(hr["hrt"])
    
    def get_ftp(self):
        power=self.get_base("power"); 
        if not power: return None

        if not "ftp" in power:
            raise Exception("ftp field not found in profile "+self.profile_date+"/"+self.discipline+"/ftp")            
        return int(power["ftp"])
    
    
    def get_par(self, base_name, par_name, obligatory=True):
        discipline=self.get_selected_discipline()   
        if not base_name in discipline: return None        
        base=discipline[base_name]        
        if not "par_name" in base:
            if obligatory:
                raise Exception(par_name+" field not found in profile "+self.profile_date+"/"+self.discipline+"/"+base_name)      
            else: return None
        return base[par_name];    
    
    def get_base(self, base_name, obligatory=False):
        discipline=self.get_selected_discipline()   
        if not base_name in discipline: 
            if obligatory:
                raise Exception(base_name+" field not found in profile "+self.profile_date+"/"+self.discipline+"/"+base_name)
            else: return None        
        base=discipline[base_name]        
        return base
    
    def get_zones(self,zones_type):    
        base=self.get_base(zones_type)      
        if not base: return None                  
        if not "zones" in base: return None        
        return base["zones"]
    
    
    def designate_zone(self, zone_type, value):        
        zones=self.get_zones(zone_type)
        if not zones: return None    
        ls=zones.items()            
        
        if zone_type=='hr' or zone_type=='power':                               
            ls.append(("my_hr",value))
            ls.sort(key=lambda v:float(v[1]))
            i=ls.index(("my_hr",value))
            if i>0: i=i-1
            return ls[i][0]
        
        if zone_type=='pace':
            value=ut.speed_ms2pace_seckm(value);
            _ls=[];
            for i,zo in enumerate(ls):
                if ls[i][1]:
                    _ls.append((ls[i][0],ut.hourmin2sec(ls[i][1])))   
                else: _ls.append(( ls[i][0],float("inf") ))
            ls=_ls            
            ls.append(("my_hr",value))          
            ls.sort(key=lambda pace: pace[1], reverse=True)
            i=ls.index(("my_hr",value))
            if i==0 : return None
            i=i-1
                    
        # res=ls[i][0]
        #    if res=="my_hr":
        #        print ls[i][0]
        #        print ls
            return ls[i][0]
        
    def get_selected_discipline(self):
        profile=self.get_selected_profile()        
        return profile[self.discipline]
        

    def get_pace_by_zone(self, zone):
        zones=self.get_zones("pace")
  #      print zones
        if zone in zones:
            return zones[zone]
        else: return None
        
        
    def get_hr_by_zone(self, zone):
        zones=self.get_zones("hr")
  #      print zones
        if zone in zones:
            return zones[zone]
        else: return None        
        
    
    
    def reset(self):         
        self.discipline=None
        self.profile_date=None
        self.activity_date=None
        
        
      
        
        
        
"""
    def get_selected_discipline():
        profile=self.get_selected_profile()
        if not self.discpipline in profile:
            print "Discipline "+self.discipline+" not found in"+self.profile
            return False;
        return profile[self.discipline]
   
#    def getPace():
        discipline=self.get_selected_discipline()
        if discipline==False: return False
        if not "pace" in discipline:
            return None;
        return discipline["pace"]
    
#    def getPaceThreshold(self):
        pace=sel.getPace()
        if pace==False or pace==None: return pace
        
        if not "ftp" in pace:
            print "ftp field not found in pace"
            return False;
                
        if not not "discpipline in profile:
            print "Discipline "+self.discipline+" not found in"+self.profile
            return None;
"""        
        
         
    

class SportSummarize(ut.mBase):
    lowdb=None
    summ={}
    dbobj=None
    delta_speed_buf_size=5
    delta_altitude_buf_size=2;
    delta_altitude_abs_max_run=1;
    delta_desc_red_factor=0.0

    def __init__(self, csvstring=None, profiles=None, parent=None, context=None):
            ut.mBase.__init__(self, parent)
            
            if csvstring:
                self.csv=pd.read_csv(StringIO(csvstring), sep=",")
                        
            self.profiles=profiles                                 
            if context:                
                self.set_context(context)
                
            self.summ={}
            self.lowdb=db.lowDBAccess(self)
            self.dbobj=dbAccess(self)

  
   
                                                
    def execute(self, user, delta_time_reset=True):
        
        key={"id": None, "discipline": None}
        key_string=None;
        summ={};
        mass=None
      
        avg_delta=1;
        tp_date_time=None;
        altitude=None
        prev_altitude=None
        delta_altitude=None
        
        pmodel_1=runPowerModel_1()
        
        delta_altbuf=ut.cyclingBuffor(self.delta_altitude_buf_size)
        delta_speedbuf=ut.cyclingBuffor(self.delta_speed_buf_size)
        
        for i, row in self.csv.iterrows():  
          try:
       #         print row
                prev_tp_date_time=tp_date_time;
                prev_altitude=altitude                
                tp_date_time=ut.xsd_dateTime2datetime(row["date_time"])  #trackpoint date_time from tcx_xml field "time"
                altitude=row["altitude_m"]
  
                
        # dyscipline change
                if key["id"] != row["id"] or key["discipline"] != row["discipline"]:                     
                     if key_string: self.funlog().logger.info("The activity " + key_string + " processed")
                     key["id"] = row["id"]
                     key["discipline"] = row["discipline"]
                     key_string=key["id"]+" "+key["discipline"];       
                     discipline=row["discipline"]
                     prev_tp_date_time=None
                     prev_altitude=None
                     altitude=None
                     delta_altitude=None
                     prev_speed=None
                     delta_altbuf.reset()
                     delta_speedbuf.reset()
                     self.profiles.select_profile_by_date(tp_date_time.strftime("%Y-%m-%d"))
                     self.profiles.select_discipline(row["discipline"])                                      
   
                     if not key_string in summ: summ[key_string]={"duration": 0,"rTSS" :0 ,"rTSScover": 0, "hrTSS" :0, "hrTSScover": 0, "pTSS":0, "pTSScover":0 ,"pace" : { }, "hr" : {}, "power": {}}

                     summ[key_string]=defaultdict(lambda :0)
                     summ[key_string]["pace"]={"zones_total" :{}, "zones_flow" : {}}
                     summ[key_string]["pace"]["zones_total"]=defaultdict(lambda: 0)  
                     summ[key_string]["pace"]["zones_m1_total"]=defaultdict(lambda: 0)  
                     summ[key_string]["hr"]={"zones_total" :{}, "zones_flow" : {}}
                     summ[key_string]["hr"]["zones_total"]=defaultdict(lambda :0)
                     summ[key_string]["power"]={"zones_total" :{}, "zones_flow" : {}}
                     summ[key_string]["power"]["zones_total"]=defaultdict(lambda :0)
                     summ[key_string]["discipline"]=key["discipline"]         
                     summ[key_string]["start_date_time"]=ut.xsd_dateTime2datetime(row["date_time"])
                     mass=self.dbobj.db_get_mass(user, summ[key_string]["start_date_time"])    
                     summ[key_string]["mass"]=mass

                     di=0;                     
                     info={"rTSS" : 0, "hrTSS":0, "TSS" : 0, "sTSS" :0 }
                     
                  #   sys.exit(0)
            
        # lap reset
                if row["type"]=="lap": 
       #             print "lap reset " + str(tp_date_time)
                    tp_date_time=None 
                    prev_tp_date_time=None 
                    altitude=None
                    prev_altitude=None
                    delta_altitude=None
                    prev_speed=None
                    delta_altbuf.reset()
                    delta_speedbuf.reset()
                                   
                delta_altitude=0
                if prev_tp_date_time and row["type"]=="trackpoint":
                    delta_time_sec=(tp_date_time-prev_tp_date_time).total_seconds()   
                    if altitude!=None and prev_altitude!=None:
                        delta_altitude=altitude-prev_altitude
                    
                  
                    
       # delta reset                       
                   
                    if delta_time_sec > 100*avg_delta and delta_time_reset:
#                        print "delta reset: "+ str(tp_date_time)+" delta: " +str(delta_time_sec) + " avg_delta "+str(avg_delta)                       
                        tp_date_time=None 
                        prev_tp_date_time=None 
                        altitude=None
                        prev_altitude=None  
                        delta_altbuf.reset()
                        prev_speed=None
                        delta_altbuf.reset()
                        delta_speedbuf.reset()
                        continue

                    summ[key_string]["duration"]=summ[key_string]["duration"]+delta_time_sec
                    if delta_altitude!=None:
                        if not m.isnan(delta_altitude):
                            if delta_altitude>0:
                                summ[key_string]["asc"]=summ[key_string]["asc"]+delta_altitude
                            if delta_altitude<0: 
                                summ[key_string]["desc"]=summ[key_string]["desc"]+delta_altitude
                            delta_altbuf.add(delta_altitude)
                            
                    avg_delta=summ[key_string]["duration"]/(di+1)
                    
        # counting on pace base                        
                    if discipline=="Running" or discipline=="Swimming":           
                        speed=row["speed"]                         
                        if not m.isnan(speed):
                            if prev_speed!=None:
                                delta_speed=prev_speed-speed;
                                delta_speedbuf.add(delta_speed)
                            prev_speed=speed;
                                
                            if speed >0:
                                if discipline=="Running":
                                    xtp=self.profiles.get_rtp()                               
                                if discipline=="Swimming":
                                    xtp=self.profiles.get_stp()
                                if not xtp==None: xtp=ut.hourmin2sec(xtp);                             
    
                                if discipline=="Running":
                                    pace=ut.speed_ms2pace_seckm(speed)   #pace[s/km]
                                    summ[key_string]["distance"]+=speed*delta_time_sec
                                if discipline=="Swimming": 
                                    pace=ut.speed_ms2pace_sec100m(speed)   #pace[s/100m]      
                              
                                avg_delta_speed=delta_speedbuf.avg(full=True)
    
                                if xtp:                            

                                    if discipline=="Running":
                                        pace_m1=pace                            
        
                                        avg_delta_alt=delta_altbuf.avg(full=True)
                                        if avg_delta_alt!=None and avg_delta_speed!=None and mass!=None:   
                                            if avg_delta_speed<0: avg_delta_speed=0
                                            if(avg_delta_alt>self.delta_altitude_abs_max_run): avg_delta_alt=self.delta_altitude_abs_max_run
                                            if(avg_delta_alt<-self.delta_altitude_abs_max_run): avg_delta_alt=-self.delta_altitude_abs_max_run                                    
                                            if(avg_delta_alt<0): avg_delta_alt=avg_delta_alt*self.delta_desc_red_factor;                                    
                                            pace_m1=pmodel_1.get_norm_pace(mass,float(pace), float(avg_delta_alt)/delta_time_sec, avg_delta_speed, speed, delta_time_sec)   # [s/km]                                     
                                            intfm1=float(xtp)/float(pace_m1)
                                            drTSSm1=(delta_time_sec * intfm1*intfm1 * 100)/3600
                                            
                                            summ[key_string]["rTSScover_m1"]=summ[key_string]["rTSScover_m1"]+delta_time_sec                         
                                            summ[key_string]["rTSSm1"]=summ[key_string]["rTSSm1"]+drTSSm1
                                           
                                            zone_m1=self.profiles.designate_zone("pace", 1000/pace_m1)
                                            if zone_m1:                                        
                                                summ[key_string]["pace"]["zones_m1_total"][zone_m1]+=delta_time_sec
        
        #                                print pace,pace_m1, delta_altitude, avg_delta_alt, avg_delta_speed, speed, delta_time_sec
                                                                  
                                    
                                    if discipline=="Running":
                                        intf=float(xtp)/float(pace)
                                        drTSS=(delta_time_sec * intf*intf * 100)/3600
                                        summ[key_string]["rTSS"]=summ[key_string]["rTSS"]+drTSS
                                        summ[key_string]["rTSScover"]=summ[key_string]["rTSScover"]+delta_time_sec  
                                        
                                    if discipline=="Swimming":                   
                                        intf=float(xtp)/float(pace)
                                        dsTSS=(delta_time_sec * intf*intf*intf * 100)/3600
                                        summ[key_string]["sTSS"]=summ[key_string]["sTSS"]+dsTSS
                                        summ[key_string]["sTSScover"]=summ[key_string]["sTSScover"]+delta_time_sec  
                    
                                    
      
                                    
                                    
     
                                # tu wyjątek na Swimmming
                                
                                zone=self.profiles.designate_zone("pace", speed)
                                if zone:
                                    summ[key_string]["pace"]["zones_total"][zone]+=delta_time_sec
                                                            
             
                       
                            
        #counting on hr base    
                    hr=row["heartrate_bpm"]
                    if not m.isnan(hr):
                        hrt=self.profiles.get_hrt()                       
                        if hrt:            
                            summ[key_string]["hrTSScover"]=summ[key_string]["hrTSScover"]+delta_time_sec                                                     
                            intf=hr/hrt
                            dhrTSS=(delta_time_sec * intf*intf * 100)/3600
                            summ[key_string]["hrTSS"]=summ[key_string]["hrTSS"]+dhrTSS                               
                              
                        zone=self.profiles.designate_zone("hr", hr)
                        if zone:
                            summ[key_string]["hr"]["zones_total"][zone]+=delta_time_sec                            

        #counting on power base    
                    power=row["power"]                  
                    if not m.isnan(power):                     
                        ftp=self.profiles.get_ftp()     
                        if ftp:  
                            summ[key_string]["TSScover"]=summ[key_string]["TSScover"]+delta_time_sec                                                     
                            intf=power/ftp
                            dftpTSS=(delta_time_sec * intf*intf * 100)/3600
                            summ[key_string]["TSS"]=summ[key_string]["TSS"]+dftpTSS        
                    di=di+1;                                      
          except Exception as e:
              if key_string: ks=key_string
              else: ks=""
              self.funlog().logger.info("Activity "+ks+" process error")
              raise e, None, sys.exc_info()[2]
                              
        if key_string: self.funlog().logger.info("The activity " + key_string + " processed")
        self.summ=summ;                            
        return True
            
    def get_summarize(self):
        return self.summ
    

    def persist(self,user_name, version, override=True):
        self.lowdb.autocommit_off()
        sql="";

       
        try:
              
            for key, value in self.summ.items():     
                persist_type="persisted"                 
                ext_id=key+" "+version
                r_act=self.dbobj.db_get_activity(user_name, ext_id)
                if r_act:
                    if not override:
                        self.funlog().logger.error("The activity "+ext_id+" exists in db. NOT persisted in db. Use override option")    
                        continue
                    else:
                        self.dbobj.db_del_activity(user_name, ext_id)
                        persist_type="overriden" 
                    
                if "distance" in value:
                    distance=value["distance"]
                else: distance="null"    
                       
                
                sql='INSERT INTO  "DWG06302"."ACTIVITY" ("USER_ID", "DISCIPLINE_ID","EXTERNAL_ID","DURATION", "START_DATE_TIME", "VERSION", "DISTANCE") VALUES((SELECT user_id FROM "USERS" WHERE email=\'{email}\'), (SELECT discipline_id FROM "DISCIPLINE" WHERE discipline_name=\'{discipline}\'), \'{_ext_id}\', \'{duration}\',\'{_start_date_time}\', \'{_version}\', {_distance});'.format(email=user_name, discipline=value["discipline"], _ext_id=ext_id, duration=value["duration"], _start_date_time=value["start_date_time"], _version=version, _distance=distance)                                
      #          print sql             
                self.lowdb.execute_sql(sql)                
                id_act=self.lowdb.get_last_id()
                id_rTSS=None;
                id_hrTSS=None;
                id_TSS=None;
                id_sTSS=None;
         #       print id_act
                            
                sql = 'INSERT INTO  "DWG06302"."LOAD" ("LOAD_VALUE","ACTIVITY_ID","LOAD_COVER","LOAD_TYPE_ID","LOAD_NORM_ID")  VALUES(?,?,?, (SELECT load_type_id FROM "LOAD_TYPE" WHERE load_type_value=?), (SELECT load_norm_id FROM "LOAD_NORM" WHERE load_norm_value=?))';
 #               sql = 'INSERT INTO  "DWG06302"."LOAD" ("LOAD_VALUE","ACTIVITY_ID","LOAD_COVER","LOAD_TYPE_ID","LOAD_NORM_ID")  VALUES(?,	(SELECT ID FROM (SELECT Identity_val_Local() as  id FROM sysibm.sysdummy1)), ?, (SELECT load_type_id FROM "LOAD_TYPE" WHERE load_type_value=\'?\'), (SELECT load_norm_id FROM "LOAD_NORM" WHERE load_norm_value=\'?\'))';

                if "rTSS" in value:
          #              print "rTSS"   
                        stmt = ibm_db.prepare(db.db, sql)
                        ibm_db.bind_param(stmt, 1, value["rTSS"])
                        ibm_db.bind_param(stmt, 2, id_act)      
                        ibm_db.bind_param(stmt, 3, value["rTSScover"])                  
                        ibm_db.bind_param(stmt, 4, "rTSS")
                        ibm_db.bind_param(stmt, 5, "none")                             
                        ibm_db.execute(stmt)   
                        id_rTSS=self.lowdb.get_last_id()
    
                           
                if "hrTSS" in value:
    
                        stmt = ibm_db.prepare(db.db, sql)
                        ibm_db.bind_param(stmt, 1, value["hrTSS"])
                        ibm_db.bind_param(stmt, 2, id_act)
                        ibm_db.bind_param(stmt, 3, value["hrTSScover"])
                        ibm_db.bind_param(stmt, 4, "hrTSS")
                        ibm_db.bind_param(stmt, 5, "none")
                        ibm_db.execute(stmt)      
                        id_hrTSS=self.lowdb.get_last_id()  
 

                if "TSS" in value:
          
                        stmt = ibm_db.prepare(db.db, sql)
                        ibm_db.bind_param(stmt, 1, value["TSS"])
                        ibm_db.bind_param(stmt, 2, id_act)
                        ibm_db.bind_param(stmt, 3, value["TSScover"])                    
                        ibm_db.bind_param(stmt, 4, "TSS")
                        ibm_db.bind_param(stmt, 5, "none")   
                        ibm_db.execute(stmt)    
                        id_TSS=self.lowdb.get_last_id()  
                        
                if "sTSS" in value:
            
                        stmt = ibm_db.prepare(db.db, sql)
                        ibm_db.bind_param(stmt, 1, value["sTSS"])
                        ibm_db.bind_param(stmt, 2, id_act)
                        ibm_db.bind_param(stmt, 3, value["sTSScover"])
                        ibm_db.bind_param(stmt, 4, "sTSS")
                        ibm_db.bind_param(stmt, 5, "none")                       
                        ibm_db.execute(stmt)       
                        id_sTSS=self.lowdb.get_last_id()  


                if "rTSSm1" in value:
          #              print "rTSS_m1"                           
                        stmt = ibm_db.prepare(db.db, sql)
                        ibm_db.bind_param(stmt, 1, value["rTSSm1"])
                        ibm_db.bind_param(stmt, 2, id_act)      
                        ibm_db.bind_param(stmt, 3, value["rTSScover_m1"])                  
                        ibm_db.bind_param(stmt, 4, "rTSS")
                        ibm_db.bind_param(stmt, 5, "model1")                                                     
                        ibm_db.execute(stmt)   
                        id_rTSSm1=self.lowdb.get_last_id()
                        
                        
                if "pace" in value:
                   if "zones_total" in value["pace"]:
                       for zone, ztime in value["pace"]["zones_total"].items():
                           sql_zones_total = 'INSERT INTO  "DWG06302"."ZONES" ("ZONE_NAME","ZONE_ORDER","LOAD_ID","ZONE_TIME","TOTAL") VALUES(\'{_zone}\', 1, {_id_rTSS}, {_time}, 1);'.format(_zone=zone, _time=int(ztime), _id_rTSS=id_rTSS);                             
                           self.lowdb.execute_sql(sql_zones_total)      
                           
                if "hr" in value:
                   if "zones_total" in value["hr"]:
                       for zone, ztime in value["hr"]["zones_total"].items():             
                           sql_zones_total = 'INSERT INTO  "DWG06302"."ZONES" ("ZONE_NAME","ZONE_ORDER","LOAD_ID","ZONE_TIME","TOTAL") VALUES(\'{_zone}\', 1, {_id_hrTSS}, {_time}, 1);'.format(_zone=zone, _time=int(ztime), _id_hrTSS=id_hrTSS);                    
                           self.lowdb.execute_sql(sql_zones_total)       
                           
                if "power" in value:
                   if "zones_total" in value["power"]:
                       for zone, ztime in value["power"]["zones_total"].items():
                          # print zone   
                           sql_zones_total = 'INSERT INTO  "DWG06302"."ZONES" ("ZONE_NAME","ZONE_ORDER","LOAD_ID","ZONE_TIME","TOTAL") VALUES(\'{_zone}\', 1, {_id_TSS}, {_time}, 1);'.format(_zone=zone, _time=int(ztime), _id_TSS=id_TSS);
#
                           self.lowdb.execute_sql(sql_zones_total)       

                if "sTSS_zones" in value:
                   if "zones_total" in value["sTSS_zones"]:
                       for zone, ztime in value["sTSS_zones"]["zones_total"].items():
                   #        print zone   
                           sql_zones_total = 'INSERT INTO  "DWG06302"."ZONES" ("ZONE_NAME","ZONE_ORDER","LOAD_ID","ZONE_TIME","TOTAL") VALUES(\'{_zone}\', 1, {_id_sTSS}, {_time}, 1);'.format(_zone=zone, _time=int(ztime), _id_sTSS=id_sTSS);
  
                           self.lowdb.execute_sql(sql_zones_total)                                   
                                 
                self.funlog().logger.info("The activity "+key+" "+ persist_type+" in db")            
                        
                 
    
        except Exception as e:
            self.funlog().logger.error("persist sql error")            
            self.lowdb.rollback()
            self.lowdb.autocommit_on()            
            raise e, None, sys.exc_info()[2]
                                        
        self.lowdb.commit()      
        self.lowdb.autocommit_on()      
        
       
class runPowerModel_1():
  
    """
        pace=[s/km]
        power=[W]
    """

    pace=[225000000, 1125, 750, 562.5, 450, 375, 300, 225, 150]
    power=[70,179,233,286,610,719,883,1150,1690]
    power_m=[]
    pace_r=[]
    power_r=[]
    power_m_r=[]

    base_mass=60
  

    
    #def __init__(self):
  #      self.power_r=self.power
  #      self.power_r.reverse()
    #    self.pace_r=self.pace
    #    self.pace_r.reverse()
        
    


    def get_norm_pace(self, mass, pace_ms, delta_alt_m, avg_delta_speed, speed, dt, cad=180, step_h=0.1, step_factor=0.2):
        p1=self.get_power(mass,pace_ms, cad, step_h, step_factor)
        p2=mass*9.81*delta_alt_m
        #p3=mass*(avg_delta_speed/dt)*(speed*dt)/dt
        p3=mass*avg_delta_speed*speed
      #  print p3
        pc=p1+p2+p3
     
        pace_v=self.pace
        power_v=self.power        

        pm=self.get_power_vert_step(mass,cad, step_h, step_factor)
        pbm=self.get_power_vert_step(self.base_mass,cad, step_h, step_factor)

 #       print pm
 #       print pbm
            
  #      print power_v
        for i,p in enumerate(power_v,0):  
            power_v[i]=p-pbm+pm
        
      
        
        pace_v.reverse()
        power_v.reverse()            
        
 #       print power_v
 #       print pace_v
 #       print p1,p2,mass,delta_alt_m
                
        pace_res=ut.interp(pc, power_v, pace_v)     
 #       print "p1=",p1
 #       print "p2=",p2
 #       print "pace_ms=", pace_ms        
 #       print "pace_res=",pace_res
        return pace_res
        
    
    def get_power(self, mass,pace_ms, cad=180, step_h=0.1, step_factor=0.2):
        
        pace_v=self.pace
        power_v=self.power
#        l=len(power_v)
#        print power_v
        pm=self.get_power_vert_step(mass,cad, step_h, step_factor)
        pbm=self.get_power_vert_step(self.base_mass,cad, step_h, step_factor)
        for i,p in enumerate(power_v,0):  
            power_v[i]=p-pbm+pm
             
    
        pace_v.reverse()
        power_v.reverse()
        
    #    print pace_v  
    #    print power_v
        
        
        p1=ut.interp(pace_ms, pace_v, power_v)        

    #    ph=self.get_power_vert_step(mass,cad,step_h, step_factor)
 #       ph_bm=self.get_power_vert_step(self.base_mass,cad,step_h, step_factor)
#        p2=p1-ph_bm
        #p3=p1
 #       print "p1=",p1
   #     print "ph_bm=",ph_bm
  #      print "ph=", ph
    #    print "p2=",p2
   #     print "p3=",p3
        return p1
        

      

    
    def get_pace(self, mass,power):
        pass
    
    def get_power_vert_step(self,mass,cad, step_h, step_factor):        
        cad_sec=float(cad)/60
        asc_sec=cad_sec*step_h
        ph=mass*9.81*asc_sec*step_factor
        return ph
    
  
        
        
"""   
    def set_mass(self,mass):
        self.mass=mass    
        pm=self.get_power_vert_step(self.mass,cad, step_h, step_factor)
        pbm=self.get_power_vert_step(self.base_mass,cad, step_h, step_factor)

        power_v=self.power

        self.power_m=[]

        for i,p in enumerate(power_v,0):  
            self.power_m.append=p-pbm+pm
            
        self.pace_m_r=self.power_m
        self.pace_m_r.reverse()            
"""    
            
        
    
    
                


#class SportProfile:
    
    
#class Discipline:
    
#class PaceZones:
    
#class HRZones:

#class PowerZones: