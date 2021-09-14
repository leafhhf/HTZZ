import sqlalchemy
import pandas as pd
import pymysql

pymysql.install_as_MySQLdb()
import warnings

warnings.filterwarnings('ignore')
import datetime
import time

engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()   #创建游标
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    sql_cmd = '''
    SELECT
        province,
        YEAR as year,
        count(*) AS num 
    FROM
        ai_incentive_policy 
    WHERE
        field like '%{}%' 
    GROUP BY
        province,
    year
    
    '''.format(field)

    df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)
        df_SQL = df_SQL.loc[df_SQL["province"] != "台湾"]
        df_SQL = df_SQL.loc[df_SQL["province"] != "澳门"]
        df_SQL = df_SQL.loc[df_SQL["province"] != "香港"]
        df_SQL = df_SQL.loc[df_SQL["province"] != "国家"]
        # df['found_year'] = pd.to_datetime(df['found_year'],format='%Y')
        # df["found_year"].astype(str).astype(int)


        df_pivot = pd.pivot_table(df_SQL, index="year", columns="province", values="num")
        df_pivot = df_pivot.fillna(0)
        df_pivot_cumsum = df_pivot.cumsum()
        df = df_pivot_cumsum.T


        df_all = pd.DataFrame()
        df2 = pd.DataFrame()
        a = df.columns
        now_year = int(datetime.datetime.now().strftime('%Y'))
        # min_year = int(a[0])
        for i in range(0, df.shape[1]):
            # i=0
            if int(a[i]) >= 2016 and int(a[i]) < now_year:
                df1 = df.iloc[:, i]
                df1 = pd.DataFrame(df1)
                df1.reset_index(drop=False, inplace=True)
                df1.columns = ["name", 'original_self_value']
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
                df2 = pd.concat([df1, df_not_list], axis=0, ignore_index=True)
                # df2.reset_index(inplace=True,drop=False)
                df2["year"] = a[i]+"年"
                df2["original_max_value"] = df1["original_self_value"].max()
                df2["index_name"] = "鼓励型政策数量"
                df2["index_num"] = "2_7"
                df2["index_id"] = "33"
                df2["type"] = "1"
                df2["field"] = field
                df2["value"] = 0.0
                for j in range(len(df2)):
                    if df2["original_self_value"][j] == 0:
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

        df_all.reset_index(inplace = True,drop = True)
        df_all.to_sql('industry_develop_index_ue', con=engine, if_exists='append', index=False)
        print("{}入库成功".format(field))
        print("-" * 50)
    else:
        print("未读取到{}的数据".format(field))


cursor.execute('delete from industry_develop_index where index_num ="2_7"')
print("已删除industry_develop_index中指标值")
sql = '''
insert into industry_develop_index(year,name,value,original_self_value,original_max_value,index_id, index_num,index_name,sequence,type,field)
select year,name,value,original_self_value,original_max_value,index_id,index_num,index_name,sequence,type,field from industry_develop_index_ue where index_num ='2_7'
'''
cursor.execute(sql)
conn.commit()  # 提交，以保存执行结果
print("已将ue数据插入到industry_develop_index")




