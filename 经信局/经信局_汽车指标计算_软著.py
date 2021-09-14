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
engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')

com = pd.read_excel(r"C:\Users\Administrator\Desktop\python_work\经信局\car_beijing.xlsx",sheet_name="终版")
company_list = com["com_name"].unique().tolist()

##读取软著数
sql_cmd1= '''
SELECT NAME as com_name,num 
FROM
	(
	SELECT list_cid,count(*) AS num 
	FROM
		dm_company_copyright_reg_chaifen 
	WHERE
		list_cid IN (
		SELECT
			cid 
		FROM
			gold_data.dm_company 
		WHERE
		NAME IN ( {} )) 
	GROUP BY
		list_cid 
	) a
	LEFT JOIN gold_data.dm_company b ON a.list_cid = b.cid'''.format(','.join(["'%s'"%item for item in company_list]))
df_SQL1=  pd.read_sql(sql=sql_cmd1, con=engine1)

### 读取节点信息
sql_cmd2= '''
SELECT last_point,com_name
FROM jxj_zhongqiyan_ue_similiarity'''
df_SQL2=  pd.read_sql(sql=sql_cmd2, con=engine2)


df_SQL =  pd.merge(df_SQL1, df_SQL2, on="com_name", how="inner")