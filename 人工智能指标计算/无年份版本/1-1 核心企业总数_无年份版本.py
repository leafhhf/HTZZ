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
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')


sql_cmd ='''select province,found_year,count(*) as number
from ai_company_info 
where industry_all like '%人工智能%'
group by province,found_year 
order by province, found_year,number desc 
'''
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)



df_SQL = df_SQL.loc[df_SQL["province"] != "台湾"]
df_SQL = df_SQL.loc[df_SQL["province"] != "澳门"]
df_SQL = df_SQL.loc[df_SQL["province"] != "香港"]
# df['found_year'] = pd.to_datetime(df['found_year'],format='%Y')
# df["found_year"].astype(str).astype(int)



df_all=pd.DataFrame()

df_pivot = pd.pivot_table(df_SQL, index="found_year", columns="province",values="number")
df_pivot = df_pivot.fillna(0)
df_pivot_cumsum = df_pivot.cumsum()
df = df_pivot_cumsum.T
a =df.columns
for i in range(0,df.shape[1]):
    df1 = df.iloc[:,i]
    df1 = pd.DataFrame(df1)
    df1.reset_index(drop=False, inplace=True)
    df1.columns = ["name",'original_self_value']
    df1['original_self_value']= round(df1['original_self_value'],2)
    df1["year"] = a[i]+"年"
    df1["original_max_value"] = df1["original_self_value"].max()
    df1["index_name"] = "核心企业总数"
    df1["index_num"] = "1_1"
    df1["index_id"] = "6"
    df1["type"] = "0"
    df1["field"] = "人工智能"
    df1["sequence"] = "0"
    df1["value"] = " "
    for j in range(len(df1)):
        df1["value"][j] = round((df1["original_self_value"][j] / df1["original_max_value"][j]) * 100,2)
    df1.sort_values(by=['value'], ascending=False, inplace=True)
        # df1["value"][j] = ('%.1f' % df1["value"][j])
    df_all = pd.concat([df1,df_all],axis=0)




        #
df_all.to_sql('industry_develop_index', con=engine, if_exists='append', index=False)



