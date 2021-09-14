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

df1 = pd.read_csv(r"C:\Users\Administrator\Desktop\11111.csv")

df_pivot = pd.pivot_table(df1, index="com_name", columns="last_point", values="sum")
df_pivot = df_pivot.fillna(0)


df = copy.deepcopy(df_pivot)

df_top5 = pd.DataFrame()
df_left= pd.DataFrame()
df1=pd.DataFrame()
df2=pd.DataFrame()
for i in range(0, df.shape[1]):
    df1 = df.iloc[:, i]
    df1 = pd.DataFrame(df1)
    df1.reset_index(drop=False, inplace=True)
    df1.columns = ["com_name", 'sum']
    df1["last_point"] = df.columns[i]
    df1 = df1.loc[df1["sum"] != 0]
    df1.sort_values(by="sum",ascending=False,inplace=True)
    df1.reset_index(inplace=True,drop=True)
    df1['rank'] = df1['sum'].rank(ascending=0, method='min')
    # length = len(df1)
    df2 = df1.loc[df1['rank'] <= 5]
    df3 = df1.loc[df1['rank'] > 5]
    df_top5 = pd.concat([df2, df_top5], axis=0)
    df_left = pd.concat([df3, df_left], axis=0)
    df_top5["reason"] = "企业产业竞争力前5"



df_final = pd.DataFrame()
df_final = pd.concat([df_top5, df_final], axis=0)
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
### 读取单项冠军
sql_cmd1 = "select batch,type,level,company_name as com_name from china_danxiangguanjun_company "
df_SQL =pd.read_sql(sql=sql_cmd1, con=engine)

df_join  =  pd.merge(df_SQL, df_left, on="com_name", how="inner")
df_champion = df_join[["com_name","sum","last_point","rank"]]
df_champion["reason"] = df_join["level"] +df_join["batch"] +df_join["type"]
df_final = pd.concat([df_champion, df_final], axis=0)


### 读取专精特新
sql_cmd2 = "select batch,pub_doc_name,level,company_name as com_name from china_zhuanjingtexin_xiaojuren_company "
df_SQL =pd.read_sql(sql=sql_cmd2, con=engine)

df_join  =  pd.merge(df_SQL, df_left, on="com_name", how="inner")
df_zhuanjing = df_join[["com_name","sum","last_point","rank"]]
df_zhuanjing["reason"] = df_join["level"] +df_join["batch"] +df_join["pub_doc_name"]

df_final = pd.concat([df_zhuanjing, df_final], axis=0)

### 读取独角兽
sql_cmd3 = "select pub_year,company_name as com_name from china_dujiaoshou_company "
df_SQL =pd.read_sql(sql=sql_cmd3, con=engine)

df_join  =  pd.merge(df_SQL, df_left, on="com_name", how="inner")
df_zhuanjing = df_join[["com_name","sum","last_point","rank"]]
df_zhuanjing["reason"] = df_join["level"] +df_join["batch"] +df_join["pub_doc_name"]

df_final = pd.concat([df_zhuanjing, df_final], axis=0)

#去掉重复行

df_unique =pd.DataFrame()
com_unique = df_final["com_name"].unique().tolist()
point_unique = df_final["last_point"].unique().tolist()
for com in com_unique:
    df_com = df_final.loc[df_final["com_name"] == com]
    point_unique = df_final["last_point"].unique().tolist()
    for point in point_unique:
        df_join = df_com.loc[df_com["last_point"] == point]
        df_join.reset_index(inplace=True,drop=True)
        reason_list = df_join["reason"].to_list()
        reason = []
        i= 0
        while i < len(reason_list):
            if i == 0:
                reason = reason_list[i]
                i += 1
            else:
                reason = reason+"、"
                reason= reason + reason_list[i]
                i += 1
        df_together = df_join.iloc[0:1,]
        df_together["total_reason"] = reason
        df_together = df_together.drop('reason',axis = 1)
        df_unique = pd.concat([df_together, df_unique], axis=0)


df_unique.sort_values(by = ["last_point","sum"],ascending=[True,False],inplace=True)
df_unique.reset_index(inplace=True,drop=True)



df_unique.to_excel("龙头企业.xlsx")