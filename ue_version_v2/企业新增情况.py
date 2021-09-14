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
# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()   #创建游标


field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    sql_cmd ='''
   SELECT found_year as year,count(*) as new_company_num FROM  kjb_de_company where label like "%{}%" 
   and found_year != "" and not isnull(found_year)  
   and found_year != "0000"  
   group by found_year order by found_year

	'''.format(field)

    # print(sql_cmd)
    df_SQL = pd.read_sql(sql=sql_cmd, con=engine)


    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)


        # 计算累计值
        df_SQL["total_company_num"] = df_SQL["new_company_num"].cumsum()
        df_SQL["year"] = df_SQL["year"] .astype("int")
        now_year = int(datetime.datetime.now().strftime('%Y'))
        df =  df_SQL.loc[(df_SQL["year"] >2010) & (df_SQL["year"] <now_year ) ]
        df["feild"] = field
        df["year"] = df["year"].astype("str")
        df["year"] = df["year"] + "年"
        df["type"] = "0"
        df["version"] = "国信优易企业工商数据"
        df["version_id"] = "2"
        cursor.execute('delete from ai_waterfall_graph where type ="0" and feild="{}"'.format(field))
        conn.commit()  # 提交，以保存执行结果
        print("已删除指标值")
        df.to_sql('ai_waterfall_graph', con=engine, if_exists='append', index=False)
        print("已将数据插入")


