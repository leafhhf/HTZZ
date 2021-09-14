import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
import copy


# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')


field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    sql_cmd ='''
    select * from ai_tax_rate
    '''
    df_SQL = pd.read_sql(sql=sql_cmd, con=engine)


    # you
    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)

        df_SQL["value"] = df_SQL["value"].astype("int")
        df_SQL.drop(["id"],axis=1,inplace=True)
        a = int(df_SQL["year"].min())
        now_year = int(datetime.datetime.now().strftime('%Y'))
        list_year = list(year for year in range(a, now_year))
        df_all=pd.DataFrame()
        df2 = pd.DataFrame()
        for i in range(0,len(list_year)):
            # print(i)
            if i ==0:
                df2 = copy.deepcopy(df_SQL)
                df2["index_name"] = "企业税率"
                df2["index_num"] = "2_8"
                df2["index_id"] = "34"
                df2["type"] = "1"
                df2["field"] = field
                df2.sort_values(by=['value'], ascending=False, inplace=True)
                df2.reset_index(drop=True, inplace=True)
                df_all = pd.concat([df2, df_all], axis=0)
            elif list_year[i] <now_year:
                df2 = copy.deepcopy(df_SQL)
                df2.drop(["year"],axis=1,inplace=True)
                df2["index_name"] = "企业税率"
                df2["index_num"] = "2_8"
                df2["index_id"] = "34"
                df2["type"] = "1"
                df2["field"] = field
                df2["year"] = list_year[i]
                df2.sort_values(by=['value'], ascending=False, inplace=True)
                df2.reset_index(drop=True, inplace=True)
                df_all = pd.concat([df2, df_all], axis=0)

        df_all.to_sql('industry_develop_index_ue', con=engine, if_exists='append', index=False)
        print("{}入库成功".format(field))
        print("-" * 50)
    else:
        print("未读取到{}的数据".format(field))




