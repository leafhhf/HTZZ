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
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')
# engine3 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()

#-------------------------------------------1、国信优易企业年报数据版本，依据dm_company_annual_report_caibao------------
field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
similarity_list = ['similarity_2','similarity_9','similarity_10','similarity_7','similarity_4',
                   'similarity_3','similarity_6','similarity_11','similarity_5','similarity_sum']
for f in range(0,len(field_list)):
    #第一步 先读取核心企业的销售收入
    sql_cmd1 ='''SELECT
	report_year as year,

	round( sum( sum_sale ), 2 ) AS com_sum
    FROM
	(
	SELECT
		{} * total_sales AS sum_sale,
		report_year

	FROM
		`kjb_de_company_annual_report_caibao` a
		LEFT JOIN kjb_de_company b ON a.cid = b.cid 
	WHERE
		a.label LIKE "%{}%" 
		AND NOT isnull( a.province ) 
		and a.province != ""
		AND total_sales != "0" 
	AND NOT isnull( total_sales )
	and report_year >2015 ) t 
    GROUP BY
	report_year
	
    ORDER BY
	report_year,
	com_sum DESC
	'''.format(similarity_list[f],field_list[f])
    df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine)
    df_SQL1["year"] = df_SQL1["year"] .astype("int")
    # 第二步：读取当年全部的销售收入值
    sql_cmd2 = '''SELECT report_year as year ,sum(total_sales) as total_sum FROM `kjb_de_company_annual_report_caibao` where report_year >=2015 group by report_year order by report_year
       	'''
    df_SQL2 = pd.read_sql(sql=sql_cmd2, con=engine)
    df_SQL2["year"] = df_SQL2["year"].astype("int")

    # 第三步：读取当年GDP值
    sql_cmd3 = '''SELECT substr(year,1,4) as year,GDP * 10000 as gdp  FROM `ai_gdp_data` where year >=2015  and province = "全国"
           	'''
    df_SQL3 = pd.read_sql(sql=sql_cmd3, con=engine)
    df_SQL3["year"] = df_SQL3["year"].astype("int")

    # 第四步：再读取当年的增加值率
    sql_cmd4 = '''SELECT
   	rate,year from kjb_de_cpi
   	'''
    df_SQL4 = pd.read_sql(sql=sql_cmd4, con=engine)
    df_SQL4["year"] = df_SQL4["year"].astype("int")
    df_SQL4["rate"] = df_SQL4["rate"].astype("float")

    df_SQL12 = pd.merge(df_SQL1, df_SQL2, on="year", how="inner")
    df_SQL123 = pd.merge(df_SQL12, df_SQL3, on="year", how="inner")
    df_SQL = pd.merge(df_SQL123, df_SQL4, on="year", how="inner")



    if len(df_SQL1) > 0 and len(df_SQL2) > 0 and len(df_SQL3) > 0 and len(df_SQL4) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field_list[f]))
        print("-" * 50)

        df_SQL["toatl_sum/gdp"] = df_SQL["total_sum"] / df_SQL["gdp"]
        df_SQL["economy_scale"] = ((df_SQL["com_sum"] / df_SQL["toatl_sum/gdp"]) * df_SQL["rate"]) / 10000
        df = df_SQL[["economy_scale", "year"]]
        df["YEAR"] = ""
        df["type"] = "0"
        df["industry"] = field_list[f]
        df["speed"] = 0.00
        df["version"] = "沪深财报数据+相似度"
        df["version_id"] = "1"
        df.sort_values(by="year", ascending=True, inplace=True)
        df.reset_index(inplace=True, drop=True)
        for i in range(len(df_SQL)):
            df["YEAR"][i] = str(df["year"][i]) + "年"
            if i == 0:
                df["speed"][i] = 0
            else:
                df["speed"][i] = round(
                    ((df["economy_scale"][i] - df["economy_scale"][i - 1]) / df["economy_scale"][i - 1]) * 100, 2)

        now_year = int(datetime.datetime.now().strftime('%Y'))
        df = df.loc[(df["year"] > 2015) & (df["year"] < now_year)]
        df_final = df[["economy_scale", "type", "YEAR", "speed", "industry", "version", "version_id"]]
        cursor.execute(
            'delete from ai_total_economy_scale_table where type=0 and industry like "%{}%"'.format(field_list[f]))
        print("已删除中指标值")
        conn.commit()  # 提交，以保存执行结果
        df_final.to_sql('ai_total_economy_scale_table', con=engine, if_exists='append', index=False)
        print("已入库{}指标值".format(field_list[f]))

#-------------------------------------------沪深财报版本---------------------------------
























