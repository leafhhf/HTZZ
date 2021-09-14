import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')



engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd =  '''select province as 省份,classify as 级别, count(*) as 数量
from ai_platform_base_lab 
where  field='人工智能'  
and found_year <='2020年' 
group by province, classify
'''
df = pd.read_sql(sql=sql_cmd, con=engine)
df.head()

df["系数"]= " "
df["平台基地实验室数量调整"]= " "

for i in range(len(df)):
    if df["级别"][i] == "国家级":
        df["系数"][i] = 1
    elif df["级别"][i] == "省级":
        df["系数"][i] = 0.8
    elif df["级别"][i] == "省市级":
        df["系数"][i] = 0.8
    else:
        df["系数"][i] = 0.5

for i in range(len(df)):
    df["平台基地实验室数量调整"][i]= df["数量"][i] * df["系数"][i]

df_group = df.groupby(by="province").agg({"平台基地实验室数量调整":sum})

df_group.reset_index(drop=False,inplace=True)

df_group["最大值"] = df_group["平台基地实验室数量调整"].max()
df_group.sort_values(by="平台基地实验室数量调整",ascending=False,inplace=True)



df_group["指标值"] = " "
for i in range(len(df_group)):
    df_group["指标值"][i] = (df_group["平台基地实验室数量调整"][i] / df_group["最大值"][i]) * 100
    df_group["指标值"][i] = ('%.1f'% df_group["指标值"][i])


print(df)
