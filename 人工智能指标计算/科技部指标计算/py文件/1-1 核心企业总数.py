import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd ='''select province,count(*) as number
from ai_company_info 
where industry_all like '%人工智能%'
and found_year<='2020' 
group by province
order by number'''

df = pd.read_sql(sql=sql_cmd, con=engine)


# 重命名各列
df.rename(columns = {"province": "省份","number" : "企业数"},inplace=True)
#提取31省份最大值
df["最大值"] = df["企业数"].max()
df.sort_values(by="企业数",ascending=False,inplace=True)
df.reset_index(drop=True,inplace=True)

# 计算指标值
df["指标值"] = " "
for i in range(len(df)):
    df["指标值"][i] = (df["企业数"][i] / df["最大值"][i]) * 100
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
df.insert(loc=0,column="年份",value=list)


print(df)