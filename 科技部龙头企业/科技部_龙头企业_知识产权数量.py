import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import copy

# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')

# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')
# engine = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/data_tmp')
# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')

# conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='rawdata')
# cursor = conn.cursor()
field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
similarity_list = ['similarity_2','similarity_9','similarity_10','similarity_7','similarity_4','similarity_3',
                   'similarity_6','similarity_11','similarity_5','vague_similarity']
for f in range(len(field_list)):

###专利数据
    sql_cmd1 = '''select b.name,num*{} as pat from (
    select cid,count(*) as num  from kjb_de_patent where com_label like "%{}%"  group by cid order by num desc) a left join kjb_de_company b on a.cid = b.cid where not isnull(name) '''.format(similarity_list[f],field_list[f])
    df_SQL1 =  pd.read_sql(sql=sql_cmd1, con=engine)
    # df_SQL1 = df_SQL1.loc[df_SQL1["reg_capital"] != ""]
    # df_SQL1.reset_index(inplace= True,drop = True)

    ###软著数据
    sql_cmd2 = '''select name,num*{} as reg from (
    select list_cid,count(*) as num  from (
    select * from dm_company_copyright_reg_chaifen where list_cid in (
    select cid from kjb_de_company where label like "%{}%") ) a group by list_cid order by num desc) b left join kjb_de_company c on b.list_cid = c.cid where not isnull(name) '''.format(similarity_list[f],field_list[f])
    df_SQL2 =  pd.read_sql(sql=sql_cmd2, con=engine)


    df_SQL12 = pd.merge(df_SQL1,df_SQL2, on="name", how="outer")
    print("读取{}数据成功".format(field_list[f]))
    df_SQL12 =  df_SQL12.fillna(0)
    df_SQL12["original_self_value"] = round(df_SQL12["pat"] +df_SQL12["reg"],2)




    df_SQL12.sort_values("original_self_value",ascending=False,inplace=True)
    df_SQL12.reset_index(inplace =True,drop=True)
    df_SQL12["field"] = field_list[f]


    # df_pivot = pd.pivot_table(df_SQL1, index="name", columns="last_point",values="value")
    # df_pivot = df_pivot.fillna(0)
    df = df_SQL12[["name","original_self_value","field"]]
    df.columns = ["com","original_self_value","field"]

    df["original_max_value"] = df["original_self_value"].max()
    df["original_self_value"] = round(df["original_self_value"],2)
    df["index_name"] = "企业知识产权数量"
    df["index_num"] = "1_3"


    df["value"] = ""
    df["value"] = df["original_self_value"].rank(method="min",ascending=False)
    # for j in range(len(df)):
    #     print(j)
    #     if df["original_self_value"][j] == 0:
    #         df["value"][j] = 0
    #     else:
    #         df["value"][j] = round((df["original_self_value"][j] / df["original_max_value"][j]) * 100, 2)

    df.to_sql('kjb_longtou_com', con=engine, if_exists='append', index=False)
    print("{}入库成功".format(field_list[f]))

# df.to_excel("经信局_注册资本指标计算结果.xlsx")