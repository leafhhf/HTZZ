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
	province,
	round( sum( sum_sale ), 2 ) AS num 
    FROM
	(
	SELECT
		{} * total_sales AS sum_sale,
		report_year,
		a.province 
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
	report_year,
	province 
    ORDER BY
	report_year,
	num DESC,
	province'''.format(similarity_list[f],field_list[f])
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
    # df_SQL3 = pd.concat([df_SQL1,df_SQL2],axis=1)
    df_SQL = pd.merge(df_SQL1,df_SQL2,on = "year",how="inner")

    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field_list[f]))
        print("-" * 50)
        df_SQL["rate"] = df_SQL["rate"].astype("float")
    # 省份处理
        df_SQL["new_province"] = ""
        for i in range(len(df_SQL)):
            if df_SQL["province"][i]:
                if df_SQL["province"][i] != "黑龙江省" and df_SQL["province"][i] != "内蒙古自治区":
                    df_SQL["new_province"][i] = df_SQL["province"][i][:2]
                else:
                    df_SQL["new_province"][i] = df_SQL["province"][i][:3]

        df_SQL = df_SQL.loc[df_SQL["new_province"] != "台湾"]
        df_SQL = df_SQL.loc[df_SQL["new_province"] != "澳门"]
        df_SQL = df_SQL.loc[df_SQL["new_province"] != "香港"]
        # df_SQL["total_sales"] = df_SQL["total_sales"].astype("float")
        # df_SQL["report_year"]= df_SQL["report_year"].astype("int")
        # df_group  = df_SQL.groupby(["report_year","new_province"]).sum("num")
        df_pivot = pd.pivot_table(df_SQL, index=["year", "new_province"], values=["num","rate"], aggfunc=np.sum)
        df_pivot["economy_scale"] = round((df_pivot["num"]*df_pivot["rate"])*100,2)
        #
        df_pivot.reset_index(inplace=True,drop=False)
        df_pivot1=  pd.pivot_table(df_pivot, index="new_province",columns="year", values="economy_scale")
        df_pivot1 = df_pivot1.fillna(0)
        # df = df_pivot1.T
        df = copy.deepcopy(df_pivot1)
        # df = df_pivot.T

        a = df.columns
        now_year = int(datetime.datetime.now().strftime('%Y'))
        df_all = pd.DataFrame()
        df2 = pd.DataFrame()
        df1_1 = pd.DataFrame()
        df1_2 = pd.DataFrame()
        df1_3 = pd.DataFrame()
        df1_4 = pd.DataFrame()
        df1_5 = pd.DataFrame()
        df1_6 = pd.DataFrame()
        df1_7 = pd.DataFrame()
        for i in range(0, df.shape[1]):
            if int(a[i]) >= 2016 and int(a[i]) < now_year:
                df1 = df.iloc[:, i]
                df1 = pd.DataFrame(df1)
                df1.reset_index(drop=False, inplace=True)
                df1.columns = ["province", 'economy_scale']

                province_list = ["黑龙江", "辽宁", "吉林", "河北", "河南", "湖北", "湖南", "山东", "山西", "陕西", "安徽", "浙江", "江苏", "福建", \
                                 "广东", "海南", "四川", "云南", "贵州", "青海", "甘肃", "江西", "内蒙古", "宁夏", "新疆", "西藏", "广西", "北京",
                                 "上海", "天津", "重庆"]
                notin_list_name = []
                notin_list_value = []
                for b in province_list:
                    if b not in df1["province"].tolist():
                        notin_list_name.append(b)
                        notin_list_value.append(0)
                df_not_list = pd.DataFrame({'province': notin_list_name, 'economy_scale': notin_list_value})
                df2 = pd.concat([df1, df_not_list], axis=0, ignore_index=True)
                # df2.reset_index(inplace=True,drop=False)
                df2["year"] = str(a[i]) + "年"
                # df2["original_max_value"] = df1["original_self_value"].max()
                # df2["index_name"] = "核心企业销售收入占比"
                # df2["index_num"] = "1_10"
                # df2["index_id"] = "15"
                df2["type"] = "0"
                df2["label"] = "0"
                df2["version"] = "国信优易企业财报数据+相似度"
                df2["industry"] = "{}".format(field_list[f])
                df2.sort_values(by=['economy_scale'], ascending=False, inplace=True)
                df2.reset_index(drop=True, inplace=True)
                #把大区加上
                # 东北大区
                df1_1 = df2.loc[(df2["province"] == "黑龙江") |(df2["province"] == "吉林")|(df2["province"] == "辽宁")]
                df1_1["label"] = 1
                df1_1["province"] = "东北"
                df1_1["economy_scale"] = df1_1["economy_scale"].sum()
                df1_1_1 = df1_1.iloc[:1,]
                # 华东大区
                df2_1 = df2.loc[(df2["province"] == "上海") | (df2["province"] == "江苏") | (df2["province"] == "浙江") |\
                                (df2["province"] == "安徽") | (df2["province"] == "福建") | (df2["province"] == "江西") |(df2["province"] == "山东")]
                df2_1["label"] = 1
                df2_1["province"] = "华东"
                df2_1["economy_scale"] = df2_1["economy_scale"].sum()
                df2_1_1 = df2_1.iloc[:1, ]
                # 华北大区
                df3_1 = df2.loc[(df2["province"] == "北京") | (df2["province"] == "天津") | (df2["province"] == "山西") |\
                                (df2["province"] == "内蒙古") | (df2["province"] == "河北") ]
                df3_1["label"] = 1
                df3_1["province"] = "华北"
                df3_1["economy_scale"] = df3_1["economy_scale"].sum()
                df3_1_1 = df3_1.iloc[:1, ]
                # 华中大区
                df4_1 = df2.loc[(df2["province"] == "河南") | (df2["province"] == "湖北") | (df2["province"] == "湖南")]
                df4_1["label"] = 1
                df4_1["province"] = "华中"
                df4_1["economy_scale"] = df4_1["economy_scale"].sum()
                df4_1_1 = df4_1.iloc[:1, ]
                # 华南大区
                df5_1 = df2.loc[(df2["province"] == "广东") | (df2["province"] == "广西") | (df2["province"] == "海南") ]
                df5_1["label"] = 1
                df5_1["province"] = "华南"
                df5_1["economy_scale"] = df5_1["economy_scale"].sum()
                df5_1_1 = df5_1.iloc[:1, ]
                # 西南大区
                df6_1 = df2.loc[(df2["province"] == "四川") | (df2["province"] == "贵州") | (df2["province"] == "云南") |\
                                (df2["province"] == "西藏") | (df2["province"] == "重庆") ]
                df6_1["label"] = 1
                df6_1["province"] = "西南"
                df6_1["economy_scale"] = df6_1["economy_scale"].sum()
                df6_1_1 = df6_1.iloc[:1, ]
                # 西北大区
                df7_1 = df2.loc[(df2["province"] == "陕西") | (df2["province"] == "甘肃") | (df2["province"] == "青海") |\
                                (df2["province"] == "宁夏") | (df2["province"] == "新疆") ]
                df7_1["label"] = 1
                df7_1["province"] = "西北"
                df7_1["economy_scale"] = df7_1["economy_scale"].sum()
                df7_1_1 = df7_1.iloc[:1, ]

            df_all = pd.concat([df2, df1_1_1,df2_1_1,df3_1_1,df4_1_1,df5_1_1,df6_1_1,df7_1_1,df_all], axis=0)

        df_all.to_sql('ai_province_economy_scale_table_test', con=engine, if_exists='append', index=False)
        print("-" * 50)
        print("{}入库成功".format(field_list[f]))
    else:
        print("未读取到{}的数据".format(field_list[f]))
cursor.execute('delete from ai_province_economy_scale_table where type =0 and label=0 and version="国信优易企业财报数据+相似度"')
cursor.execute('delete from ai_province_economy_scale_table where type =0 and label=1 and version="国信优易企业财报数据+相似度"')
print("已删除中指标值")
sql1 = '''
insert into ai_province_economy_scale_table(year,province,economy_scale,label,type,industry)
select year,province,economy_scale,label,type,industry  from ai_province_economy_scale_table_test where type =0 and label=0
'''
sql2 = '''
insert into ai_province_economy_scale_table(year,province,economy_scale,label,type,industry)
select year,province,economy_scale,label,type,industry  from ai_province_economy_scale_table_test where type =0 and label=1
'''
cursor.execute(sql1)
cursor.execute(sql2)
conn.commit()  # 提交，以保存执行结果
print("已将数据插入")



#-------------------------------------------沪深财报版本---------------------------------
























