import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')


engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd =  '''select province, count(*) as number
from ai_incentive_policy 
where year<='2020' 
and field='人工智能' 
group by province
order by number desc
'''
df = pd.read_sql(sql=sql_cmd, con=engine)
df.head()


df.rename(columns = {"province": "省份","number" : "数量"},inplace=True)
df["最大值"] = df["数量"].max()
df.sort_values(by="数量",ascending=False,inplace=True)
df.reset_index(drop=True,inplace=True)


df["指标值"] = " "
for i in range(len(df)):
    df["指标值"][i] = (df["数量"][i] / df["最大值"][i]) * 100
    df["指标值"][i] = ('%.1f'% df["指标值"][i])


#把年份信息插入
list = []
i= 0
while True:
    if i < len(df):
        list.append("2020年")
        i += 1
    else:
        break

df.insert(loc=0,column="年度",value=list)
print(df)
