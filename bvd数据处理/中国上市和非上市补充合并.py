import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
import numpy as np

# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
sql_cmd ='''select * from bvd_china_company_info_original_final '''

df = pd.read_sql(sql=sql_cmd, con=engine)

#  将n.a.值替换一下
df = df.replace("n.a.","")
# 不同年份的数据
df1= df[['company_name','country','yanfatouru2020','rongzi2020','yingyeshouru2020',"bvd_index_num",'found_date']]
df1 = df1.replace("0.0","")
df1.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df1["year"] = "2020"

df2=df[['company_name','country','yanfatouru2019','rongzi2019','yingyeshouru2019',"bvd_index_num",'found_date']]
df2 = df2.replace("0.0","")
df2.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df2["year"] = "2019"
#df2.head()

df3= df[['company_name','country','yanfatouru2018','rongzi2018','yingyeshouru2018',"bvd_index_num",'found_date']]
df3 = df3.replace("0.0","")
df3.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df3["year"] = "2018"
#df3.head()

df4= df[['company_name','country','yanfatouru2017','rongzi2017','yingyeshouru2017',"bvd_index_num",'found_date']]
df4 = df4.replace("0.0","")
df4.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df4["year"] = "2017"


df5= df[['company_name','country','yanfatouru2016','rongzi2016','yingyeshouru2016',"bvd_index_num",'found_date']]
df5 = df5.replace("0.0","")
df5.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df5["year"] = "2016"


df6= df[['company_name','country','yanfatouru2015','rongzi2015','yingyeshouru2015',"bvd_index_num",'found_date']]
df6 = df6.replace("0.0","")
df6.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df6["year"] = "2015"


df7= df[['company_name','country','yanfatouru2014','rongzi2014','yingyeshouru2014',"bvd_index_num",'found_date']]
df7 = df7.replace("0.0","")
df7.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df7["year"] = "2014"


df8= df[['company_name','country','yanfatouru2013','rongzi2013','yingyeshouru2013',"bvd_index_num",'found_date']]
df8 = df8.replace("0.0","")
df8.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df8["year"] = "2013"


df9= df[['company_name','country','yanfatouru2012','rongzi2012','yingyeshouru2012',"bvd_index_num",'found_date']]
df9 = df9.replace("0.0","")
df9.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df9["year"] = "2012"


df10= df[['company_name','country','yanfatouru2011','rongzi2011','yingyeshouru2011',"bvd_index_num",'found_date']]
df10 = df10.replace("0.0","")
df10.columns=[['company_name','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df10["year"] = "2011"



# df = pd.read_csv("C:/Users/Administrator/Desktop/BVD中国_按bvd索引新增数据.csv")
# df.sort_values(["bvd_index_num","id"],inplace=True)
# df=df.fillna(0)
# df = df.replace("n.a.",0)
#
# df_group = df.groupby("bvd_index_num").count()

