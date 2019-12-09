# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 21:44:34 2019

@author: klosmarc
"""

import ibm_db
import m_logger as log
import sys
import m_utils as ut

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
_isautapp=True;



def init(context):
    global db
    try:
      if not db:
            db=ibm_db.connect("DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net;PORT=50000;PROTOCOL=TCPIP;UID=dwg06302;PWD=8f+pmtbxknntg59p", "", "")
    except Exception as e:
        context.funlog().logger.error("db connection error")
        raise e, None, sys.exc_info()[2]
        
    return True

def close():
    global db
    if db:
        return ibm_db.close(db)


class lowDBAccess(ut.mBase):
    
    num_row_aff=None
    
    def __init__(self,parent=None):
        ut.mBase.__init__(self, parent)


    def execute_sql(self,sql_com):
        self.isautapp()
        try:
            global db
            stmt=ibm_db.exec_immediate( db, sql_com)

            if stmt:         
                self.num_row_aff=ibm_db.num_rows(stmt)          
            return stmt
        except Exception as e: 
            self.funlog().logger.error("database error")
            raise e, None, sys.exc_info()[2]
            
            
    def execute_sql_stmt(self,sql):       # results: stmt; None: error; Exception: error
        self.isautapp()
        try:     
            global db
            stmt = ibm_db.prepare(db, sql)
            res = ibm_db.execute(stmt)
            if res: return stmt
            else: return None
        except Exception as e: 
            self.funlog().logger.error("database error")
            raise e, None, sys.exc_info()[2]
    
    def autocommit_off(self):
        self.isautapp()
        global db
        return ibm_db.autocommit(db, ibm_db.SQL_AUTOCOMMIT_OFF)
    
    def autocommit_on(self):
        self.isautapp()
        global db
        return ibm_db.autocommit(db, ibm_db.SQL_AUTOCOMMIT_ON)
    
    def commit(self):
        self.isautapp()
        global db
        return ibm_db.commit(db)
    
    def rollback(self):
        self.isautapp()
        global db
        return ibm_db.rollback(db)
    
    def get_last_id(self):   
        self.isautapp()
        res=self.execute_sql("SELECT Identity_val_Local() as  id FROM sysibm.sysdummy1;")
        return ibm_db.fetch_tuple(res)[0];
    
    def isautapp(self):
        return True
    
    def get_num_row_aff(self):
        return self.num_row_aff
        
        
    

    