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
engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
# engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()   #创建游标

field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    sql_cmd ='''
    SELECT  public_date, applicant_country_code as country FROM ai_pct WHERE field LIKE "%{}%"
	'''.format(field)

    # print(sql_cmd)
    df_SQL = pd.read_sql(sql=sql_cmd, con=engine)


    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)
        df_SQL['public_date'] = pd.to_datetime(df_SQL['public_date'], errors='coerce')
        # data['found_date'] = data['found_date'].apply(lambda x: pd.to_datetime(x).strftime('%m/%d/%Y')[0])
        df_SQL['public_year'] = pd.to_datetime(df_SQL['public_date'], format='%Y-%m-%d  %H:%M:%S').dt.year

        df_SQL1 = df_SQL[["public_year","country"]]
        df_SQL1 = df_SQL1.fillna(0)
        df_SQL2 = df_SQL1.loc[df_SQL1["public_year"] != 0 ]
        df_SQL2.reset_index(inplace=True,drop=True)

        list_country = []
        list_year = []
        for i in range(0, len(df_SQL2)):
            if len(df_SQL2["country"][i]) > 0:
                a = df_SQL2["country"][i].split(",")
                for j in a:
                    # print(j)
                    b = j.strip()
                    list_country.append(b)
                    list_year.append(df_SQL2["public_year"][i])
                    a = []

        # 将list转换成dataframe
        df_country = pd.DataFrame({"country": list_country, "pub_year": list_year})
        df_count = df_country.value_counts().sort_values(ascending=False)
        df2 = pd.DataFrame(data=df_count, columns=["num"])
        df2.reset_index(inplace=True, drop=False)
        df2["country"].loc[df2["country"] == "西班牙"] = "欧盟"
        df2["country"].loc[df2["country"] == "荷兰"] = "欧盟"
        df2["country"].loc[df2["country"] == "芬兰"] = "欧盟"
        df2["country"].loc[df2["country"] == "卢森堡"] = "欧盟"
        df2["country"].loc[df2["country"] == "比利时"] = "欧盟"
        df2["country"].loc[df2["country"] == "爱尔兰"] = "欧盟"
        df2["country"].loc[df2["country"] == "瑞典"] = "欧盟"
        df2["country"].loc[df2["country"] == "丹麦"] = "欧盟"
        df2["country"].loc[df2["country"] == "波兰"] = "欧盟"
        df2["country"].loc[df2["country"] == "葡萄牙"] = "欧盟"
        df2["country"].loc[df2["country"] == "奥地利"] = "欧盟"
        df2["country"].loc[df2["country"] == "匈牙利"] = "欧盟"
        df2["country"].loc[df2["country"] == "捷克共和国"] = "欧盟"
        df2["country"].loc[df2["country"] == "希腊"] = "欧盟"
        df2["country"].loc[df2["country"] == "罗马尼亚"] = "欧盟"
        df2["country"].loc[df2["country"] == "斯洛文尼亚"] = "欧盟"
        df2["country"].loc[df2["country"] == "克罗地亚"] = "欧盟"
        df2["country"].loc[df2["country"] == "塞浦路斯"] = "欧盟"
        df2["country"].loc[df2["country"] == "马耳他"] = "欧盟"
        df2["country"].loc[df2["country"] == "爱沙尼亚"] = "欧盟"
        df2["country"].loc[df2["country"] == "保加利亚"] = "欧盟"
        df2["country"].loc[df2["country"] == "立陶宛"] = "欧盟"
        df2["country"].loc[df2["country"] == "斯洛伐克"] = "法国"
        df2["country"].loc[df2["country"] == "拉托维亚"] = "欧盟"

        df3 = df2.loc[(df2["country"] == "阿根廷")
                      | (df2["country"] == "澳大利亚")
                      | (df2["country"] == "巴西")
                      | (df2["country"] == "加拿大")
                      | (df2["country"] == "中国")
                      | (df2["country"] == "法国")
                      | (df2["country"] == "德国")
                      | (df2["country"] == "印度")
                      | (df2["country"] == "印度尼西亚")
                      | (df2["country"] == "意大利")
                      | (df2["country"] == "日本")
                      | (df2["country"] == "韩国")
                      | (df2["country"] == "墨西哥")
                      | (df2["country"] == "俄罗斯")
                      | (df2["country"] == "沙特阿拉伯")
                      | (df2["country"] == "南非")
                      | (df2["country"] == "土耳其")
                      | (df2["country"] == "英国")
                      | (df2["country"] == "美国")
                      | (df2["country"] == "欧盟")]


        df_pivot = pd.pivot_table(df3, index="pub_year", columns="country", values="num")
        df_pivot = df_pivot.fillna(0)
        df_pivot_cumsum = df_pivot.cumsum()
        df = df_pivot.T



        ## 将df缺少年份补充
        df_year = list(df.columns)
        now_year = int(datetime.datetime.now().strftime('%Y'))
        list_year = [b for b in range(int(df_year[0]), now_year)]

        for s in range(len(list_year)):
            if list_year[s] not in df_year:
                df.insert(loc=s, column=list_year[s], value=df.iloc[:, s - 1])

        df_all = pd.DataFrame()
        a =df.columns
        now_year = int(datetime.datetime.now().strftime('%Y'))
        for i in range(0,df.shape[1]):
            if int(a[i]) >= 2016 and int(a[i]) < now_year:
                df1 = df.iloc[:,i]
                df1 = pd.DataFrame(df1)
                df1.reset_index(drop=False, inplace=True)
                df1.columns = ["name",'original_self_value']
                df1['original_self_value']= round(df1['original_self_value'],2)
                country_list = ['阿根廷','澳大利亚','巴西','加拿大','中国','法国','德国','印度','印度尼西亚','意大利',
                                 '日本','韩国','墨西哥','俄罗斯','沙特阿拉伯','南非','土耳其','英国','美国','欧盟']
                notin_list_name = []
                notin_list_value = []
                for b in country_list:
                    if b not in df1["name"].tolist():
                        notin_list_name.append(b)
                        notin_list_value.append(0)
                df_not_list = pd.DataFrame({'name': notin_list_name, 'original_self_value': notin_list_value})
                df2 = pd.concat([df1, df_not_list], axis=0, ignore_index=True)
                # df2.reset_index(inplace=True,drop=False)
                df2["year"] = str(a[i])[:4] + "年"
                df2["original_max_value"] = df1["original_self_value"].max()
                df2["index_name"] = "PCT专利数量"
                df2["index_num"] = "4_5"
                df2["index_id"] = "50"
                df2["type"] = "3"
                df2["field"] = "{}".format(field)
                df2["value"] = 0.0
                for j in range(len(df2)):
                    if df2["original_self_value"][j] == 0:
                        df2["value"][j] = 0
                    else:
                        df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100, 2)

                df2.sort_values(by=['value'], ascending=False, inplace=True)
                df2.reset_index(drop=True, inplace=True)

                # 排名
                df2["sequence"] = ""
                df2["sequence"] = df2["value"].rank(method="first",ascending=False)
                df_all = pd.concat([df2, df_all], axis=0)
                # cursor.close()  # 关闭游标
        df_all.to_sql('industry_develop_index_global_test', con=engine, if_exists='append', index=False)
        print("-" * 50)
        print("{}入库成功".format(field))

    else:
         print("未读取到{}的数据".format(field))

cursor.execute('delete from industry_develop_index_global where index_num ="4_5"')
print("已删除industry_develop_index中指标值")
sql = '''
        insert into industry_develop_index_global(year,name,value,original_self_value,original_max_value,index_id, index_num,index_name,sequence,type,field)
        select year,name,value,original_self_value,original_max_value,index_id,index_num,index_name,sequence,type,field from industry_develop_index_global_test where index_num ='4_5'
        '''
cursor.execute(sql)
conn.commit()  # 提交，以保存执行结果
print("已将ue数据插入到industry_develop_index_global")