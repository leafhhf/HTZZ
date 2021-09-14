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



sql_cmd = '''select com as com_name, yearly_sales from gaishi_car_info where not isnull(yearly_sales) '''
df_SQL2=  pd.read_sql(sql=sql_cmd, con=engine2)
df_SQL2["yearly_sales"]=df_SQL2["yearly_sales"].replace(" ","")


df_SQL2["cleaned_yearly_sales"]=""
for i in range(len(df_SQL2)):
    if df_SQL2["yearly_sale"][i].find("万人民币") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万人民币","")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
    elif df_SQL2["yearly_sale"][i].find("万美元") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万美元", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["cleaned_yearly_sale"][i] * 6.5
    elif df_SQL2["yearly_sale"][i].find(" 万元 美元") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万元 美元", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["cleaned_yearly_sale"][i] * 6.5
    elif df_SQL2["yearly_sale"][i].find("万元 人民币") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万元 人民币", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
    elif df_SQL2["yearly_sale"][i].find(" 万元") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace(" 万元", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
    elif df_SQL2["yearly_sale"][i].find("(万元)") > 0:
        df_SQL2["yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace(",", "")
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("(万元)", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
    elif df_SQL2["yearly_sale"][i].find("万元人民币") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万元人民币", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
    elif df_SQL2["yearly_sale"][i].find("万元") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万元", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
    elif df_SQL2["yearly_sale"][i].find(" 万") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace(" 万", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
    elif df_SQL2["yearly_sale"][i].find("万欧元") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万欧元", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["cleaned_yearly_sale"][i] * 7.6
    elif df_SQL2["yearly_sale"][i].find("万香港元") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万香港元", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["cleaned_yearly_sale"][i] * 1.2
    elif df_SQL2["yearly_sale"][i].find("万") > 0:
        df_SQL2["cleaned_yearly_sale"][i] = df_SQL2["yearly_sale"][i].replace("万", "")
        df_SQL2["cleaned_yearly_sale"][i] = float(df_SQL2["cleaned_yearly_sale"][i])

