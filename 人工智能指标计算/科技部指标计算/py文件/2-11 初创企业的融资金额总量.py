import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')


engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd =  '''select a.province, round(sum(b.financing_amount) ) as number
from ai_company_info a, ai_financing_amount_dup b 
where a.com_registered_name=b.com_registered_name 
and b.field like '%人工智能%'
and b.invest_year='2020' 
and a.maturity='初创型' 
group by a.province
order by number desc
'''
df = pd.read_sql(sql=sql_cmd, con=engine)

df.rename(columns = {"province": "省份","number" : "经济规模"},inplace=True)

list= ["黑龙江","辽宁","吉林","河北","河南","湖北","湖南","山东","山西","陕西","安徽","浙江","江苏","福建", \
        "广东","海南","四川","云南","贵州","青海","甘肃","江西","内蒙古","宁夏","新疆","西藏","广西","北京","上海", \
       "天津","重庆"]
df1 = pd.DataFrame(list,columns=["省份"])

df_merge = pd.merge(df1,df,on="省份",how ="outer")
df_merge = df_merge.fillna(0)
df_merge.sort_values(by="经济规模",ascending=False,inplace=True)
df_merge.reset_index(drop=True,inplace=True)


df_merge["最大值"] = df_merge["经济规模"].max()


df_merge["指标值"] = " "
for i in range(len(df_merge)):
    df_merge["指标值"][i] = (df_merge["经济规模"][i] / df_merge["最大值"][i]) * 100
    df_merge["指标值"][i] = ('%.1f'% df_merge["指标值"][i])


#把年份信息插入
list = []
i= 0
while True:
    if i < len(df_merge):
        list.append("2020年")
        i += 1
    else:
        break

df_merge.insert(loc=0,column="年度",value=list)
print(df_merge)

