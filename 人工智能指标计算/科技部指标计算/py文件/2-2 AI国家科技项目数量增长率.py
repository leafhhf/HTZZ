import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')


engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd1 =  '''select province, count(*) as number
from ai_project_info 
where tag='人工智能' 
and year<='2020年' 
and province not in  ("香港","澳门","台湾")
group by province
order by number desc
'''
df1 = pd.read_sql(sql=sql_cmd1, con=engine)
# df1.head()

sql_cmd2 =  '''select province, count(*) as number
from ai_project_info 
where tag='人工智能' 
and year<='2019年' 
and province not in  ("香港","澳门","台湾")
group by province
order by number desc 
'''
df2 = pd.read_sql(sql=sql_cmd2, con=engine)
# df2.head()

df1.rename(columns = {"province": "省份","number" : "2020年数量"},inplace=True)
df2.rename(columns = {"province": "省份","number" : "2019年数量"},inplace=True)

list= ["黑龙江","辽宁","吉林","河北","河南","湖北","湖南","山东","山西","陕西","安徽","浙江","江苏","福建", \
        "广东","海南","四川","云南","贵州","青海","甘肃","江西","内蒙古","宁夏","新疆","西藏","广西","北京","上海", \
       "天津","重庆"]
df3 = pd.DataFrame(list,columns=["省份"])
# 合并两个df

df4 = pd.merge(df3,df1,on="省份",how= "outer")
df = pd.merge(df4,df2,on="省份",how= "outer").fillna(0)


# 求增长率
df["增长量"] = " "

for i in range(len(df)):
    df["增长量"][i] = (df["2020年数量"][i]- df["2019年数量"][i] )


# 最大值
df["最大值"] = " "
df["最大值"] = df["增长量"].max()

# 指标值
df["指标值"] = " "
for i in range(len(df)):
    df["指标值"][i] = (df["增长量"][i] / df["最大值"][i]) * 100


df.sort_values(by="指标值", ascending=False, inplace=True)


for i in range(len(df)):
    df["指标值"][i] = ('%.1f' % df["指标值"][i])

df.reset_index(drop=True,inplace=True)

print(df)
