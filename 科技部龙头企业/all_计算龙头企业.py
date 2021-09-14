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
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')

df_all = pd.DataFrame()
field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    sql_cmd = '''select com,value,index_name from kjb_longtou_com where field ="{}" '''.format(field)
    df_SQL= pd.read_sql(sql=sql_cmd, con=engine)
    print("读取{}数据成功".format(field))

    df_pivot = pd.pivot_table(df_SQL, index="com", columns="index_name",values="value")
    emp_max=df_pivot['企业员工人数'].max()
    cap_max=df_pivot['企业当前注册资本规模'].max()
    know_max=df_pivot['企业知识产权数量'].max()



    df_pivot["企业员工人数"]= df_pivot["企业员工人数"].fillna(emp_max)
    df_pivot["企业当前注册资本规模"]= df_pivot["企业当前注册资本规模"].fillna(cap_max)
    df_pivot["企业知识产权数量"]= df_pivot["企业知识产权数量"].fillna(know_max)
    # df_pivot = df_pivot.fillna("无")
    df_pivot.reset_index(drop=False,inplace=True)


    df = copy.deepcopy(df_pivot)
    df["field"] = field

    df["total"]=0.0
    df["total"] = df['企业员工人数'] +df['企业当前注册资本规模'] +df['企业知识产权数量']
    df.sort_values("total",ascending = True,inplace=True)

    df["排名"] = df["total"].rank(method="min",ascending=True)


    df_all = pd.concat([df, df_all], axis=0)
    print("合并{}数据成功".format(field))

df_all.reset_index(drop=True,inplace=True)
df_all.columns=["公司名称","企业员工人数指标排名","企业当前注册资本规模指标排名","企业知识产权数量指标排名","领域","三个指标排名之和","总排名"]
df_all.to_csv("科技部龙头企业.csv")





