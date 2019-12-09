# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 21:30:25 2019

@author: klosmarc
"""

import requests 

db2id={
       "db": "BLUDB",
       "dsn": "DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net;PORT=50000;PROTOCOL=TCPIP;UID=dwg06302;PWD=8f+pmtbxknntg59p;",
       "host": "dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net",
       "hostname": "dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net",
       "https_url": "https://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:8443",
       "jdbcurl": "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB",
       "parameters": {},
       "password": "8f+pmtbxknntg59p",
       "port": 50000,
       "ssldsn": "DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net;PORT=50001;PROTOCOL=TCPIP;UID=dwg06302;PWD=8f+pmtbxknntg59p;Security=SSL;",
       "ssljdbcurl": "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50001/BLUDB:sslConnection=true;",
       "uri": "db2://dwg06302:8f%2Bpmtbxknntg59p@dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB",
       "username": "dwg06302"
       }


db=None
access_token=None
host=None
max_num_rows_fetched=1000000;

def init():         
        global access_token
        global host
        api='/dbapi/v3'
        host=db2id['https_url']+api
        
        userinfo={ 'userid' : db2id['username'], 'password': db2id['password']}
        service='/auth/tokens';
        r=requests.post(host+service, json=userinfo)
        if r==None: raise Exception("database: getting token error")
        elif r.status_code==401: raise Exception("database: getting token error, unauthorized request, status_code="+str(r.status_code))  
        elif r.status_code!=200: raise Exception("database: getting token error, status_code="+str(r.status_code))             
        access_token=r.json()["token"]
        if not access_token: raise Exception("database: no token in response")         
        return True
        

def post_sql(sql, limit=max_num_rows_fetched, stop="yes"):
        global access_token
        global host      

    
        sql_command={
                "commands": sql,
                "limit": limit,
                "separator" :";",
                "stop_on_error": stop                
            }
        
        service = "/sql_jobs"
        r=requests.post(host+service, headers=auth_header(), json=sql_command)
        if r==None: raise Exception("database: sql execute error: "+sql)    
        elif r.status_code!=201: raise Exception("database: sql execute error: status_code="+str(r.status_code)+": sql:"+sql +":result: "+r.json())                 
        
        return r

def retrieve_result():
    pass

def execute_sql(sql, limit=max_num_rows_fetched, stop="yes"):
    global access_token
    global host
    r=post_sql(sql, limit, stop)
    jobid=r.json()["id"]
    service = "/sql_jobs"
    req=host+service + "/"+jobid ;
    r=requests.get(req, headers=auth_header())
    if r==None: raise Exception("database: sql execute error: "+sql)    
    elif r.status_code!=200: raise Exception("database: sql execute error: status_code="+str(r.status_code)+": sql:"+sql+":result: "+r.json())                 

    return r

def auth_header():
     global access_token
     return {
             "Authorization" : "Bearer " + access_token
     }
    
        
def set_rollback_point():
    sql="SAVEPOINT A ON ROLLBACK RETAIN CURSORS;" + " ROLLBACK WORK TO SAVEPOINT A;"
    r=execute_sql(sql)
    if "error" in r.json()["results"][0] : raise Exception("database: rollback savepoint not set: error: "+r.json()["results"][0]["error"]+" sql: " +sql)
    
    return True;
    
def begin():
    sql="BEGIN"
    r=execute_sql(sql)
    if "error" in r.json()["results"][0] : raise Exception("database: rollback savepoint not set: error: "+r.json()["results"][0]["error"]+" sql: " +sql)
    
    return True;
    


def rollback():
    sql="ROLLBACK WORK TO SAVEPOINT A;"
    r=execute_sql(sql)
    print r.json()
    if "error" in r.json()["results"][0] : raise Exception("database: rollback not done: error: "+r.json()["results"][0]["error"]+" sql: " +sql)
    return True
    
      


def get_db():
    if not db: db=DB2()
    return b
    

def init:
    con=ibm_db.connect("DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net;PORT=50000;PROTOCOL=TCPIP;UID=dwg06302;PWD=8f+pmtbxknntg59p", "", "")



class DB2 :
    def __init__(self):
        
        api='/dbapi/v3'
        host=db2id['https_url']+api
        
        userinfo={ 'userid' : db2id['username'], 'password': db2id['password']}
        service='/auth/tokens';
        r=requests.post(host+service, json=userinfo)
        if not r: raise Exception("database: getting token error")
        elif r.status_code!=200: raise Exception("database: getting token error, status_code="+r.status_code)
        
        
        pass
    