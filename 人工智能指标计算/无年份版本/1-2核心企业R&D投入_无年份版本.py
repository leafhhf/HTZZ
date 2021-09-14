import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
sql_cmd ='''select c.year,c.areacode,round(sum(c.funding),2) as num from 
    (SELECT a.*, b.com_registered_name from  knowledge_graph.ai_hushen_caibao a 
    JOIN knowledge_graph.ai_company_info b ON a.companyname = b.com_registered_name
    WHERE b.com_field LIKE '%人工智能%' )c
    group by c.year,c.areacode
    ORDER BY areacode,year desc, num desc
'''
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)



df_SQL = df_SQL.loc[df_SQL["areacode"] != "台湾"]
df_SQL = df_SQL.loc[df_SQL["areacode"] != "澳门"]
df_SQL = df_SQL.loc[df_SQL["areacode"] != "香港"]
# df['found_year'] = pd.to_datetime(df['found_year'],format='%Y')
# df["found_year"].astype(str).astype(int)

# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')




df_pivot = pd.pivot_table(df_SQL, index="year", columns="areacode",values="num")
df = df_pivot.T
df = df.fillna(0)
a =df.columns

df_all=pd.DataFrame()
df2 = pd.DataFrame()
for i in range(0,df.shape[1]):
    df1 = df.iloc[:,i]
    df1 = pd.DataFrame(df1)
    df1.reset_index(drop=False, inplace=True)
    df1.columns = ["name",'original_self_value']
    # df1['original_self_value']= round(df1['original_self_value'],2)
    province_list = ["黑龙江", "辽宁", "吉林", "河北", "河南", "湖北", "湖南", "山东", "山西", "陕西", "安徽", "浙江", "江苏", "福建", \
                     "广东", "海南", "四川", "云南", "贵州", "青海", "甘肃", "江西", "内蒙古", "宁夏", "新疆", "西藏", "广西", "北京", "上海", \
                     "天津", "重庆"]
    notin_list_name = []
    notin_list_value = []
    for b in province_list:
        if b not in df1["name"].tolist():
            notin_list_name.append(b)
            notin_list_value.append(0)
    df_not_list = pd.DataFrame({'name': notin_list_name, 'original_self_value': notin_list_value})
    df2 = pd.concat([df1, df_not_list], axis=0,ignore_index=True)
    # df2.reset_index(inplace=True,drop=False)
    df2["year"] = a[i]+"年"
    df2["original_max_value"] = df1["original_self_value"].max()
    df2["index_name"] = "核心企业R&D投入"
    df2["index_num"] = "1_2"
    df2["index_id"] = "7"
    df2["type"] = "0"
    df2["field"] = "人工智能"
    df2["sequence"] = "0"
    df2["value"] = " "
    for j in range(len(df2)):
        if df2["original_self_value"][j] == 0:
            df2["value"][j] = 0
        else:
            df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100,2)

    df2.sort_values(by=['value'], ascending=False, inplace=True)
        # df1["value"][j] = ('%.1f' % df1["value"][j])

    df_all = pd.concat([df2,df_all],axis=0)

df_all.to_sql('industry_develop_index', con=engine, if_exists='append', index=False)



