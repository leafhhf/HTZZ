import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# 读取sql的数据
engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
# sql_cmd1 ='''select * from dm_area_code'''

engine2= sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
sql_cmd2 ='''select com_registered_name from ai_company_info where industry_all like '%人工智能%' '''
df2 = pd.read_sql(sql=sql_cmd2, con=engine2)
company_list= df2['com_registered_name'].tolist()

# engine3=sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')

# df1 = pd.read_sql(sql=sql_cmd1, con=engine1)
ll =[]
ii =0
for i in company_list:
    sql_cmd3 = "SELECT * FROM dm_company WHERE name = '{}'" .format(i)
    if len(pd.read_sql(sql=sql_cmd3, con=engine1)) ==0:
        ii = ii+1
        print(ii)
        ll.append(i)





sql_cmd3 = "SELECT * FROM dm_company WHERE name in ({}) ".format(','.join(["'%s'" % item for item in company_list]))
df3 = pd.read_sql(sql=sql_cmd3, con=engine1)
listed = df3["name"]

set1=set(listed)
set2=set(company_list)
com_listed=set1 & set2
com_not_listed=set1 ^ set2

sql_cmd4 = "SELECT company_name,count(*) FROM dm_organization_invest WHERE company_name in ({}) group by company_name order by count(*) desc ".format(','.join(["'%s'" % item for item in company_list]))
df4 = pd.read_sql(sql=sql_cmd4, con=engine1)

sql_cmd5 = "select * from dm_company_equity_info a join dm_company b on a.cid= b.cid WHERE b.name in ({}) ".format(','.join(["'%s'" % item for item in company_list]))
df5 = pd.read_sql(sql=sql_cmd5, con=engine1)



sql_cmd6 = "select  distinct cid from dm_company "
df6 = pd.read_sql(sql=sql_cmd6, con=engine1)


sql_cmd7 = "select  * from dm_company_patent a join dm_company b on a.cid= b.cid  WHERE b.name in ({}) ".format(','.join(["'%s'" % item for item in company_list]))
df7 = pd.read_sql(sql=sql_cmd7, con=engine1)



sql_cmd8 = "SELECT company_name,count(*) FROM dm_company_finance WHERE company_name in ({}) group by company_name order by count(*) desc ".format(','.join(["'%s'" % item for item in company_list]))
df8 = pd.read_sql(sql=sql_cmd8, con=engine1)

sql_cmd9 = "select * from dm_company_annual_report where report_year=2020 "
df9 = pd.read_sql(sql=sql_cmd9, con=engine1)