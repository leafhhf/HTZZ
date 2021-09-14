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
# engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
# engine2 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/data_tmp')
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()   #创建游标
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
similarity_list = ['similarity_2','similarity_9','similarity_10','similarity_7','similarity_4',
                   'similarity_3','similarity_6','similarity_11','similarity_5','vague_similarity']

for f in range(0,len(field_list)):
    #第一步 先读取各省份核心企业的销售收入
    sql_cmd1 = '''select year,areacode,round(sum(num),2) as com_sum from (
        select *,yingyeshouru * {} as num  from (
        select year,areacode,companyname,ue_label,yingyeshouru from ai_hushen_caibao
        where ue_label like "%{}%" and year > 2015) a left join kjb_de_company b on a.companyname = b.name) c  
        where areacode !="-" and not ISNULL(areacode)
        group by year,areacode order by year desc,com_sum desc
    	'''.format(similarity_list[f], field_list[f])
    df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine)
    df_SQL1["year"] = df_SQL1["year"].astype("int")

    # 第二步：读取当年各省份全部的销售收入值
    sql_cmd2 = '''SELECT year,areacode,sum(yingyeshouru) as total_sum FROM `ai_hushen_caibao` where year >2015  and areacode != "-" group by year,areacode
        	'''
    df_SQL2 = pd.read_sql(sql=sql_cmd2, con=engine)
    df_SQL2["year"] = df_SQL2["year"].astype("int")

    # 第三步：读取当年GDP值
    sql_cmd3 = '''SELECT year,GDP * 10000 as gdp,province as areacode  FROM `ai_gdp_data` where year >2015  and province !="全国"
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



    df_SQL12 = pd.merge(df_SQL1, df_SQL2, on=["year","areacode"], how="inner")
    df_SQL123 = pd.merge(df_SQL12, df_SQL3, on=["year","areacode"], how="inner")
    df_SQL = pd.merge(df_SQL123, df_SQL4, on="year", how="inner")



    if len(df_SQL1) > 0 and len(df_SQL2) > 0 and len(df_SQL3) > 0 and len(df_SQL4) > 0 :

        print("-" * 50)
        print("读取{}数据成功".format(field_list[f]))
        print("-" * 50)

        df_SQL["toatl_sum/gdp"] = df_SQL["total_sum"] / df_SQL["gdp"]
        df_SQL["economy_scale"] = ((df_SQL["com_sum"] / df_SQL["toatl_sum/gdp"]) * df_SQL["rate"]) / 10000
        df_pivot = pd.pivot_table(df_SQL, index="year", columns= "areacode", values="economy_scale")
        df = df_pivot.T

        # df_pivot.reset_index(inplace=True, drop=False)
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
                df1['economy_scale'] = round(df1['economy_scale'], 2)
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
                # df2["index_name"] = "核心企业总数"
                # df2["index_num"] = "1_1"
                # df2["index_id"] = "6"
                df2["type"] = "0"
                df2["industry"] = "{}".format(field_list[f])
                df2["label"] = "0"
                df2["version"] = "沪深财报数据+相似度"
                df2["version_id"] = "1"
                df2.sort_values(by=['economy_scale'], ascending=False, inplace=True)
                df2.reset_index(drop=True, inplace=True)
                # 把大区加上
                # 东北大区
                df1_1 = df2.loc[(df2["province"] == "黑龙江") | (df2["province"] == "吉林") | (df2["province"] == "辽宁")]
                df1_1["label"] = 1
                df1_1["province"] = "东北"
                df1_1["economy_scale"] = df1_1["economy_scale"].sum()
                df1_1['economy_scale'] = round(df1_1['economy_scale'], 2)
                df1_1_1 = df1_1.iloc[:1, ]
                # 华东大区
                df2_1 = df2.loc[(df2["province"] == "上海") | (df2["province"] == "江苏") | (df2["province"] == "浙江") | \
                                (df2["province"] == "安徽") | (df2["province"] == "福建") | (df2["province"] == "江西") | (
                                            df2["province"] == "山东")]
                df2_1["label"] = 1
                df2_1["province"] = "华东"
                df2_1["economy_scale"] = df2_1["economy_scale"].sum()
                df2_1['economy_scale'] = round(df2_1['economy_scale'], 2)
                df2_1_1 = df2_1.iloc[:1, ]
                # 华北大区
                df3_1 = df2.loc[(df2["province"] == "北京") | (df2["province"] == "天津") | (df2["province"] == "山西") | \
                                (df2["province"] == "内蒙古") | (df2["province"] == "河北")]
                df3_1["label"] = 1
                df3_1["province"] = "华北"
                df3_1["economy_scale"] = df3_1["economy_scale"].sum()
                df3_1['economy_scale'] = round(df3_1['economy_scale'], 2)
                df3_1_1 = df3_1.iloc[:1, ]
                # 华中大区
                df4_1 = df2.loc[(df2["province"] == "河南") | (df2["province"] == "湖北") | (df2["province"] == "湖南")]
                df4_1["label"] = 1
                df4_1["province"] = "华中"
                df4_1["economy_scale"] = df4_1["economy_scale"].sum()
                df4_1['economy_scale'] = round(df4_1['economy_scale'], 2)
                df4_1_1 = df4_1.iloc[:1, ]
                # 华南大区
                df5_1 = df2.loc[(df2["province"] == "广东") | (df2["province"] == "广西") | (df2["province"] == "海南")]
                df5_1["label"] = 1
                df5_1["province"] = "华南"
                df5_1["economy_scale"] = df5_1["economy_scale"].sum()
                df5_1['economy_scale'] = round(df5_1['economy_scale'], 2)
                df5_1_1 = df5_1.iloc[:1, ]
                # 西南大区
                df6_1 = df2.loc[(df2["province"] == "四川") | (df2["province"] == "贵州") | (df2["province"] == "云南") | \
                                (df2["province"] == "西藏") | (df2["province"] == "重庆")]
                df6_1["label"] = 1
                df6_1["province"] = "西南"
                df6_1["economy_scale"] = df6_1["economy_scale"].sum()
                df6_1['economy_scale'] = round(df6_1['economy_scale'], 2)
                df6_1_1 = df6_1.iloc[:1, ]
                # 西北大区
                df7_1 = df2.loc[(df2["province"] == "陕西") | (df2["province"] == "甘肃") | (df2["province"] == "青海") | \
                                (df2["province"] == "宁夏") | (df2["province"] == "新疆")]
                df7_1["label"] = 1
                df7_1["province"] = "西北"
                df7_1["economy_scale"] = df7_1["economy_scale"].sum()
                df7_1['economy_scale'] = round(df7_1['economy_scale'], 2)
                df7_1_1 = df7_1.iloc[:1, ]
                df_all = pd.concat([df2, df1_1_1, df2_1_1, df3_1_1, df4_1_1, df5_1_1, df6_1_1, df7_1_1, df_all], axis=0)

        df_all.to_sql('ai_province_economy_scale_table_test', con=engine, if_exists='append', index=False)
        print("-" * 50)
        print("{}入库成功".format(field_list[f]))

    else:
        print("未读取到{}的数据".format(field_list[f]))

cursor.execute('delete from ai_province_economy_scale_table where type =0')
print("已删除数据")
sql = '''
    insert into ai_province_economy_scale_table(year,province,economy_scale,type,label,industry,version,version_id)
    select year,province,economy_scale,type,label,industry,version,version_id from ai_province_economy_scale_table_test where type ="0"
    '''
cursor.execute(sql)
conn.commit()  # 提交，以保存执行结果
print("已入库数据")