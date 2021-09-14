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
engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')
# engine3 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()   #创建游标


field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]

similarity_list = ['similarity_2','similarity_9','similarity_10','similarity_7','similarity_4','similarity_3',
                   'similarity_6','similarity_11','similarity_5','vague_similarity']
for f in range(0,len(field_list)):
    #第一步 先读取核心企业的毛利润
    # sql_cmd1 ='''
    # SELECT
	# province,
	# report_year as year,
	# sum( total_profit ) as  profit
    # FROM
	# `kjb_de_company_annual_report_caibao`
    # WHERE
	# label LIKE "%{}%"
	# AND NOT ISNULL( province )
	# AND NOT ISNULL( total_profit )
	# AND total_profit != ""
	# AND report_year > 2015
    # GROUP BY
	# province,
	# report_year
    # ORDER BY
	# province,
	# report_year DESC'''.format(field)
    # df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine)
    sql_cmd1 ='''
    select areacode as province,year,round(sum(num),2) as profit from (
    select a.areacode,a.year,a.lirunzonge * similarity_2 as num from (
    SELECT areacode,year,lirunzonge,companyname FROM `ai_hushen_caibao` where ue_label like "%人工智能%"  
    and year >2015 and not isnull(lirunzonge)  and areacode != "-" ) a 
    left join kjb_de_company b on a.companyname = b.name ) v 
    group by areacode,year order by year desc,profit desc'''.format(similarity_list[f],field_list[f])
    df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine)




    if len(df_SQL1) > 0:
        print("√" * 50)
        print("读取{}核心企业毛利润数据成功".format(field_list[f]))
        print("√" * 50)
    else:
        print("X" * 50)
        print("读取{}核心企业毛利润数据失败".format(field_list[f]))
        print("X" * 50)

   # 第二步  再读取GDP
    sql_cmd2 = '''select SUBSTR(`year`,1,4) as year,province,GDP*10000 as GDP from ai_gdp_data where SUBSTR(`year`,1,4) >2015 '''
    df_SQL2 = pd.read_sql(sql=sql_cmd2, con=engine2)


    # 第三部 最后读取经济规模

    sql_cmd3 = '''
        SELECT
            province,
            economy_scale *10000 as economy_scale,
            substr(YEAR,1,4)as year
        FROM
            ai_province_economy_scale_table 
        WHERE
            industry = '{}' 
            AND type = 0
        '''.format(field_list[f])
    df_SQL3 = pd.read_sql(sql=sql_cmd3, con=engine)

    if len(df_SQL1) > 0:
        print("√" * 50)
        print("读取{}核心企业经济规模数据成功".format(field_list[f]))
        print("√" * 50)
    else:
        print("X" * 50)
        print("读取{}核心企业经济规模数据失败".format(field_list[f]))
        print("X" * 50)

    if len(df_SQL1) > 0 and len(df_SQL2) > 0 and len(df_SQL3):
    # # 省份处理
    #     df_SQL1["new_province"] = ""
    #     for i in range(len(df_SQL1)):
    #         if df_SQL1["province"][i]:
    #             if df_SQL1["province"][i] != "黑龙江省" and df_SQL1["province"][i] != "内蒙古自治区":
    #                 df_SQL1["new_province"][i] = df_SQL1["province"][i][:2]
    #             else:
    #                 df_SQL1["new_province"][i] = df_SQL1["province"][i][:3]
    #     df_SQL1 = df_SQL1[["year","new_province","profit"]]
    #     df_SQL1.columns = ["year","province","profit"]
    # 将三列的数据合并

        df_SQL4 = pd.merge(df_SQL1,df_SQL2,on=["year", "province"],how="inner")
        df_SQL = pd.merge(df_SQL4, df_SQL3, on=["year", "province"], how="inner")


        df_SQL = df_SQL.loc[df_SQL["province"] != "台湾"]
        df_SQL = df_SQL.loc[df_SQL["province"] != "澳门"]
        df_SQL = df_SQL.loc[df_SQL["province"] != "香港"]
        # df_SQL["total_sales"] = df_SQL["total_sales"].astype("float")
        # df_SQL["report_year"]= df_SQL["report_year"].astype("int")
        # df_group  = df_SQL.groupby(["report_year","new_province"]).sum("num")
        # df_pivot = pd.pivot_table(df_SQL, index=["year", "province"], values=["num","total_num","GDP"], aggfunc=np.sum)
        df_SQL["proportion"] = round((df_SQL["profit"]/(df_SQL["GDP"]-df_SQL["economy_scale"]))*100,2)
        df_pivot = pd.pivot_table(df_SQL, index="year",columns= "province", values="proportion")
        df_pivot = df_pivot.fillna(0)
        df = df_pivot.T

        a = df.columns
        now_year = int(datetime.datetime.now().strftime('%Y'))
        df_all = pd.DataFrame()
        df2 = pd.DataFrame()
        for i in range(0, df.shape[1]):
            if int(a[i]) >= 2016 and int(a[i]) < now_year:
                df1 = df.iloc[:, i]
                df1 = pd.DataFrame(df1)
                df1.reset_index(drop=False, inplace=True)
                df1.columns = ["name", 'original_self_value']
                df1['original_self_value'] = round(df1['original_self_value'], 2)
                province_list = ["黑龙江", "辽宁", "吉林", "河北", "河南", "湖北", "湖南", "山东", "山西", "陕西", "安徽", "浙江", "江苏", "福建", \
                                 "广东", "海南", "四川", "云南", "贵州", "青海", "甘肃", "江西", "内蒙古", "宁夏", "新疆", "西藏", "广西", "北京",
                                 "上海", "天津", "重庆"]
                notin_list_name = []
                notin_list_value = []
                for b in province_list:
                    if b not in df1["name"].tolist():
                        notin_list_name.append(b)
                        notin_list_value.append(0)
                df_not_list = pd.DataFrame({'name': notin_list_name, 'original_self_value': notin_list_value})
                df2 = pd.concat([df1, df_not_list], axis=0, ignore_index=True)
                # df2.reset_index(inplace=True,drop=False)
                df2["year"] = a[i] + "年"
                df2["original_max_value"] = df1["original_self_value"].max()
                df2["index_name"] = "核心企业毛利润占GDP比重"
                df2["index_num"] = "1_11"
                df2["index_id"] = "16"
                df2["type"] = "0"
                df2["field"] = "{}".format(field_list[f])
                # df2["sequence"] = "0"
                df2["value"] = 0.0
                for j in range(len(df2)):
                    if df2["original_self_value"][j] <= 0:
                        df2["value"][j] = 0
                    else:
                        df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100, 2)

                df2.sort_values(by=['value'], ascending=False, inplace=True)
                df2.reset_index(drop=True, inplace=True)

                # 判断梯队
                df2["sequence"] = ""
                zero_num = (df2["value"] == 0).astype(int).sum()
                thresold = int((31 - zero_num) / 3)
                yushu = (31 - zero_num) % 3
                # print("得分为0的有{}".format(zero_num))
                if zero_num <= 7:
                    df2.loc[0:7, ]["sequence"] = "第一梯队"
                    df2.loc[8:15, ]["sequence"] = "第二梯队"
                    df2.loc[16:23, ]["sequence"] = "第三梯队"
                    df2.loc[24:30, ]["sequence"] = "第四梯队"
                elif zero_num > 7 and yushu != 0:
                    df2.loc[0:thresold - 1, ]["sequence"] = "第一梯队"
                    df2.loc[thresold:thresold * 2, ]["sequence"] = "第二梯队"
                    df2.loc[(2 * thresold) + 1:31 - zero_num - 1, ]["sequence"] = "第三梯队"
                    df2["sequence"].loc[df2["value"] == 0] = "第四梯队"
                elif zero_num > 7 and yushu == 0:
                    df2["sequence"].loc[df2["value"] == 0] = "第四梯队"
                    df2.loc[0:thresold - 1, ]["sequence"] = "第一梯队"
                    df2.loc[thresold:thresold * 2 - 1, ]["sequence"] = "第二梯队"
                    df2.loc[2 * thresold:31 - zero_num - 1, ]["sequence"] = "第三梯队"
                df_all = pd.concat([df2, df_all], axis=0)

        df_all.to_sql('industry_develop_index_ue', con=engine, if_exists='append', index=False)
        print("-" * 50)
        print("{}入库成功".format(field_list[f]))
    else:
        print("未读取到{}的数据".format(field_list[f]))


cursor.execute('delete from industry_develop_index where index_num ="1_11"')
print("已删除industry_develop_index中指标值")
sql = '''
    insert into industry_develop_index(year,name,value,original_self_value,original_max_value,index_id, index_num,index_name,sequence,type,field)
    select year,name,value,original_self_value,original_max_value,index_id,index_num,index_name,sequence,type,field from industry_develop_index_ue where index_num ='1_11'
    '''
cursor.execute(sql)
conn.commit()  # 提交，以保存执行结果
print("已将ue数据插入到industry_develop_index")
