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

# conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='rawdata')
# cursor = conn.cursor()



com = pd.read_excel(r"C:\Users\Administrator\Desktop\python_work\经信局\car_beijing.xlsx",sheet_name="终版")
company_list = com["com_name"].unique().tolist()



#求公司的总专利数

sql_cmd1= '''select last_point,com_name,num_zhongqiyan from jxj_zhongqiyan_ue_similiarity'''
df_SQL=  pd.read_sql(sql=sql_cmd1, con=engine2)

df_SQL["num_zhongqiyan"] =df_SQL["num_zhongqiyan"].astype("int")
df_pivot = pd.pivot_table(df_SQL, index="com_name", columns="last_point",values="num_zhongqiyan")
df_pivot = df_pivot.fillna(0)
df = copy.deepcopy(df_pivot)

df_all = pd.DataFrame()
df1=pd.DataFrame()
for i in range(0, df.shape[1]):
    df1 = df.iloc[:, i]
    df1 = pd.DataFrame(df1)
    df1.reset_index(drop=False, inplace=True)
    df1.columns = ["com_name", 'original_self_value']

    df1= df1.loc[df1["original_self_value"] != 0 ]
    df1.sort_values(by = "original_self_value",ascending = False,inplace= True)
    df1.reset_index(inplace=True,drop= True)
    df1["original_max_value"] = df1["original_self_value"].max()

    df1["index_name"] = "企业相关领域专利申请数"
    df1["index_num"] = "1_2"
    # df1["index_id"] = "6"
    df1["last_point"] = df.columns[i]


    df1["value"] = 0.0
    for j in range(len(df1)):
        if df1["original_self_value"][j] == 0:
            df1["value"][j] = 0
        else:
            df1["value"][j] = round((df1["original_self_value"][j] / df1["original_max_value"][j]) * 100, 2)
    df_all = pd.concat([df1, df_all], axis=0)

df_all.to_sql('jxj_zhongqiyan_ue_index', con=engine2, if_exists='append', index=False)
df_all.to_excel("经信局_专利指标计算结果.xlsx")