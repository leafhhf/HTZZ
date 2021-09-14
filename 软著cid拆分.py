import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import copy

# 读取sql的数据
# engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')

# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')
engine = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/data_tmp')
# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')

sql_cmd1= '''select id,cids from  dm_company_copyright_reg where not isnull (cids)'''
df_SQL=  pd.read_sql(sql=sql_cmd1, con=engine)

list_id = []
list_cid = []
for i in range(0,len(df_SQL)):
    if len(df_SQL["cids"][i]) > 0:
        a = df_SQL["cids"][i].split(";")
        for j in a:
            b = j.strip()
            list_cid.append(b)
            list_id.append(df_SQL["id"][i])
            a = []

df = {"list_cid": list_cid, "list_id": list_id}
df = pd.core.frame.DataFrame(df)
df.to_sql('dm_company_copyright_reg_chaifen', con=engine1, if_exists='append')