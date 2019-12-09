# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 23:41:33 2019

@author: klosmarc
"""

import db2acess as db

db.init()


sql=  "SELECT * FROM activity WHERE user_id=(SELECT user_id FROM 'USERS' WHERE email=\'{_email}\') AND exernal_id=\'{_external_id}\';".format(_email=user_name, _external_id=external_id)
print sql        
res=db.db.execute_sql("SELECT * FROM activity WHERE user_id=(SELECT user_id FROM USERS WHERE email=\'{_email}\') AND external_id=\'{_external_id}\';".format(_email=user_name+"5", _external_id=external_id))
print res        
tup=ibm_db.fetch_tuple(res);
print tup