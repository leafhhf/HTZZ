import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')


engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd1 =  '''select province,count(*) as number
from ai_company_info 
where industry_all like '%人工智能%' 
and found_year<='2017'
or ifshangshi = "是"
group by province
order by number desc
'''
df1 = pd.read_sql(sql=sql_cmd1, con=engine)
# df1.head()

sql_cmd2 =  '''select province,count(*) as number
from ai_company_info 
where industry_all like '%人工智能%' 
and found_year<='2016'
or ifshangshi = "是"
group by province
order by number desc
'''
df2 = pd.read_sql(sql=sql_cmd2, con=engine)
# df2.head()

df1.rename(columns = {"province": "省份","number" : "2020年数量"},inplace=True)
df2.rename(columns = {"province": "省份","number" : "2019年数量"},inplace=True)

# 合并两个df
df = pd.merge(df1,df2,on="省份")


# 求增长率
df["增长率"] = " "
for i in range(len(df)):
    df["增长率"][i] = (df["2020年数量"][i]- df["2019年数量"][i] )/ df["2019年数量"][i]

# 最大值
df["最大值"] = " "
df["最大值"] = df["增长率"].max()

# 指标值
df["指标值"] = " "
for i in range(len(df)):
    df["指标值"][i] = (df["增长率"][i] / df["最大值"][i]) * 100


df.sort_values(by="指标值", ascending=False, inplace=True)


for i in range(len(df)):
    df["增长率"][i] = "{:.1%}".format(df["增长率"][i])
    df["最大值"][i] = "{:.1%}".format(df["最大值"][i])
    df["指标值"][i] = ('%.1f' % df["指标值"][i])

df.reset_index(drop=True,inplace=True)

print(df)
