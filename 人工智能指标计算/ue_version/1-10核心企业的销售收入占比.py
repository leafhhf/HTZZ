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



field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    #第一步 先读取核心企业的销售收入
    sql_cmd1 ='''select
    report_year, province, sum(total_sales) as num
    from dm_company_annual_report_caibao where not isnull(province) and label like "%{}%" and report_year >2015 and total_sales != "0" and not isnull(total_sales) 
    group by report_year, province
    order by report_year, province'''.format(field)
    df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine)

   # 第二步  再读取当地全部企业销售收入
    sql_cmd2 = '''select report_year, province, sum(total_sales) as total_num from dm_company_annual_report_caibao 
    where  not isnull(province) and report_year >2015 and total_sales != "0" and not isnull(total_sales) group by report_year, province
    order by report_year, province'''
    df_SQL2 = pd.read_sql(sql=sql_cmd2, con=engine)

    # df_SQL3 = pd.concat([df_SQL1,df_SQL2],axis=1,join='inner')
    df_SQL = pd.merge(df_SQL1,df_SQL2,on = ["report_year","province"],how="inner")

    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)

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
        df_pivot = pd.pivot_table(df_SQL, index=["report_year", "new_province"], values=["num","total_num"], aggfunc=np.sum)
        df_pivot["rate"] = round((df_pivot["num"]/df_pivot["total_num"])*100,2)
        #
        df_pivot.reset_index(inplace=True,drop=False)
        df_pivot1=  pd.pivot_table(df_pivot, index="new_province",columns="report_year", values="rate")
        df_pivot1 = df_pivot1.fillna(0)
        # df = df_pivot1.T



        df = copy.deepcopy(df_pivot1)
        # df = df_pivot.T

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
                df2["index_name"] = "核心企业销售收入占比"
                df2["index_num"] = "1_10"
                df2["index_id"] = "15"
                df2["type"] = "0"
                df2["field"] = "{}".format(field)
                # df2["sequence"] = "0"
                df2["value"] = 0.0
                for j in range(len(df2)):
                    if df2["original_self_value"][j] == 0:
                        df2["value"][j] = 0
                    else:
                        df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100, 2)

                df2.sort_values(by=['value'], ascending=False, inplace=True)
                df2.reset_index(drop=True, inplace=True)

                # 判断梯队
                df2["level"] = ""
                zero_num = (df2["value"] == 0).astype(int).sum()
                thresold = int((31 - zero_num) / 3)
                yushu = (31 - zero_num) % 3
                # print("得分为0的有{}".format(zero_num))
                if zero_num <= 7:
                    df2.loc[0:7, ]["level"] = "第一梯队"
                    df2.loc[8:15, ]["level"] = "第二梯队"
                    df2.loc[16:23, ]["level"] = "第三梯队"
                    df2.loc[24:30, ]["level"] = "第四梯队"
                elif zero_num > 7 and yushu != 0:
                    df2.loc[0:thresold - 1, ]["level"] = "第一梯队"
                    df2.loc[thresold:thresold * 2, ]["level"] = "第二梯队"
                    df2.loc[(2 * thresold) + 1:31 - zero_num - 1, ]["level"] = "第三梯队"
                    df2["level"].loc[df2["value"] == 0] = "第四梯队"
                elif zero_num > 7 and yushu == 0:
                    df2["level"].loc[df2["value"] == 0] = "第四梯队"
                    df2.loc[0:thresold - 1, ]["level"] = "第一梯队"
                    df2.loc[thresold:thresold * 2 - 1, ]["level"] = "第二梯队"
                    df2.loc[2 * thresold:31 - zero_num - 1, ]["level"] = "第三梯队"
                df_all = pd.concat([df2, df_all], axis=0)

        df_all.to_sql('industry_develop_index_ue', con=engine, if_exists='append', index=False)
        print("-" * 50)
        print("{}入库成功".format(field))
    else:
        print("未读取到{}的数据".format(field))