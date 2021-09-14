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
    SELECT YEAR
        ,
        country,
       count(*) AS num 
    FROM
        bvd_g20_listed_company_info_final 
    WHERE
        field LIKE "%{}%" 
    GROUP BY
        YEAR,
        country 
    ORDER BY
        country,
        YEAR,
        num DESC
	'''.format(field)

    # print(sql_cmd)
    df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)


    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)


        df_all=pd.DataFrame()
        df_pivot = pd.pivot_table(df_SQL, index="YEAR", columns="country",values="num")
        df_pivot = df_pivot.fillna(0)
        df_pivot_cumsum = df_pivot.cumsum()

        df = df_pivot_cumsum.T

        ## 将df缺少年份补充
        df_year = list(df.columns)
        now_year = int(datetime.datetime.now().strftime('%Y'))
        list_year = [str(b) for b in range(int(df_year[0]), now_year)]

        for s in range(len(list_year)):
            if list_year[s] not in df_year:
                df.insert(loc=s, column=list_year[s], value=df.iloc[:, s - 1])

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
                df2["year"] = a[i] + "年"
                df2["original_max_value"] = df1["original_self_value"].max()
                df2["index_name"] = "上市核心企业数"
                df2["index_num"] = "3_1"
                df2["index_id"] = "40"
                df2["type"] = "2"
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

cursor.execute('delete from industry_develop_index_global where index_num ="3_1"')
print("已删除industry_develop_index中指标值")
sql = '''
        insert into industry_develop_index_global(year,name,value,original_self_value,original_max_value,index_id, index_num,index_name,sequence,type,field)
        select year,name,value,original_self_value,original_max_value,index_id,index_num,index_name,sequence,type,field from industry_develop_index_global_test where index_num ='3_1'
        '''
cursor.execute(sql)
conn.commit()  # 提交，以保存执行结果
print("已将ue数据插入到industry_develop_index_global")



