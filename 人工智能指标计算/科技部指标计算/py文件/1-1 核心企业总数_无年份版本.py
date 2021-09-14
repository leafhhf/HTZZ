import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
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

engine1 = create_engine('mysql+pymysql://root:leaf960313..@localhost:3306/htzz_test')


if int(df_SQL["found_year"].max()) < datetime.datetime.now().year:
    df_pivot = pd.pivot_table(df_SQL, index="found_year", columns="province",values="number")
    df_pivot = df_pivot.fillna(0)
    df_pivot_cumsum = df_pivot.cumsum()
    df = df_pivot_cumsum.T
    a =df.columns
    for i in range(0,df.shape[1]):
        df1 = df.iloc[:,i]
        df1 = pd.DataFrame(df1)
        df1.reset_index(drop=False, inplace=True)
        df1.columns = ["province",'number']
        df1["year"] = a[i]
        df1["max"] = df1["number"].max()
        df1["score"] = " "
        for j in range(len(df1)):
            df1["score"][j] = (df1["number"][j] / df1["max"][j]) * 100
            df1["score"][j] = ('%.1f' % df1["score"][j])
        df1.to_sql('1_1_ORIGINAL_TABLE', con=engine1, if_exists='append', index=False)

