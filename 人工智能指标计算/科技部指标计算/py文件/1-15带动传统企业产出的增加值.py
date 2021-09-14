import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')


engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd =  '''select province, economy_scale as number
from ai_province_economy_scale_table 
where industry='人工智能' 
and type=1 
and year='2020年'
'''
df = pd.read_sql(sql=sql_cmd, con=engine)
df.head()


df.rename(columns = {"province": "省份","number" : "带动规模（亿元）"},inplace=True)
df["最大值"] = df["带动规模（亿元）"].max()
df.sort_values(by="带动规模（亿元）",ascending=False,inplace=True)
df.reset_index(drop=True,inplace=True)


df["指标值"] = " "
for i in range(len(df)):
    df["指标值"][i] = (df["带动规模（亿元）"][i] / df["最大值"][i]) * 100
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
