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
engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')


field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    sql_cmd ='''
    select province, pub_year,count(*) as num from (
    select province,pub_year from kjb_dc_patent 
    where not ISNULL(province) 
    and not ISNULL(pub_year) 
    and patent_label like '%{}%') a group by province, pub_year'''.format(field)
    df_SQL = pd.read_sql(sql=sql_cmd, con=engine)


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
        df_SQL['pub_year'] = df_SQL['pub_year'].astype('int')


        df_pivot = pd.pivot_table(df_SQL, index="pub_year", columns="new_province",values="num")
        df_pivot = df_pivot .fillna(0)
        df_pivot_cumsum = df_pivot.cumsum()
        df = df_pivot_cumsum.T

        ## 将df缺少年份补充

        df_year = list(df.columns)
        now_year = int(datetime.datetime.now().strftime('%Y'))
        list_year = [b for b in range(int(df_year[0]), now_year)]

        for s in range(len(list_year)):
            if list_year[s] not in df_year:
                df.insert(loc=s, column=list_year[s], value=df.iloc[:, s - 1])

        a =df.columns
        df_all=pd.DataFrame()
        df2 = pd.DataFrame()
        now_year = int(datetime.datetime.now().strftime('%Y'))
        for i in range(0, df.shape[1]):
            if int(a[i]) >= 2016 and int(a[i]) < now_year:
                df1 = df.iloc[:,i]
                df1 = pd.DataFrame(df1)
                df1.reset_index(drop=False, inplace=True)
                df1.columns = ["name",'original_self_value']
                # df1['original_self_value']= round(df1['original_self_value'],2)
                province_list = ["黑龙江", "辽宁", "吉林", "河北", "河南", "湖北", "湖南", "山东", "山西", "陕西", "安徽", "浙江", "江苏", "福建", \
                                 "广东", "海南", "四川", "云南", "贵州", "青海", "甘肃", "江西", "内蒙古", "宁夏", "新疆", "西藏", "广西", "北京", "上海", \
                                 "天津", "重庆"]
                notin_list_name = []
                notin_list_value = []
                for b in province_list:
                    if b not in df1["name"].tolist():
                        notin_list_name.append(b)
                        notin_list_value.append(0)
                df_not_list = pd.DataFrame({'name': notin_list_name, 'original_self_value': notin_list_value})
                df2 = pd.concat([df1, df_not_list], axis=0,ignore_index=True)
                # df2.reset_index(inplace=True,drop=False)
                df2["year"] = str(a[i]) +"年"
                df2["original_max_value"] = df1["original_self_value"].max()
                df2["index_name"] = "有效专利数量"
                df2["index_num"] = "1_6"
                df2["index_id"] = "11"
                df2["type"] = "0"
                df2["field"] = field
                df2["sequence"] = "0"
                df2["value"] = 0.0
                for j in range(len(df2)):
                    if df2["original_self_value"][j] == 0:
                        df2["value"][j] = 0
                    else:
                        df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100,2)

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

                df_all = pd.concat([df2,df_all],axis=0)

        df_all.to_sql('industry_develop_index_ue', con=engine, if_exists='append', index=False)
        print("{}入库成功".format(field))
        print("-" * 50)

    else:
        print("未读取到{}的数据".format(field))


