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
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()


field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    sql_cmd ='''SELECT
        province,
        found_year,
        classify,
        count(*) as num
    FROM
        ai_platform_base_lab 
    WHERE
        field like '%{}%' 
    GROUP BY
        province,
        found_year,
        classify
    '''.format(field)
    df_SQL = pd.read_sql(sql=sql_cmd, con=engine)

    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)

        df_SQL = df_SQL.loc[df_SQL["province"] != "台湾"]
        df_SQL = df_SQL.loc[df_SQL["province"] != "澳门"]
        df_SQL = df_SQL.loc[df_SQL["province"] != "香港"]
        # df_SQL = df_SQL.loc[df_SQL["province"] != "华东"]
        # df_SQL = df_SQL.loc[df_SQL["province"] != "华中"]
        # df_SQL = df_SQL.loc[df_SQL["province"] != "华北"]
        # df_SQL = df_SQL.loc[df_SQL["province"] != "华南"]
        # df_SQL = df_SQL.loc[df_SQL["province"] != "东北"]
        # df_SQL = df_SQL.loc[df_SQL["province"] != "西北"]
        # df_SQL = df_SQL.loc[df_SQL["province"] != "西南"]


        # df['found_year'] = pd.to_datetime(df['found_year'],format='%Y')
        # df["found_year"].astype(str).astype(int)


        df_pivot = pd.pivot_table(df_SQL, index=["found_year","classify"], columns="province",values="num")
        df_pivot = df_pivot .fillna(0)
        df_pivot.reset_index(inplace=True)


        ### 将不同级别算上不同的分
        df_pivot1 = df_pivot.loc[df_pivot["classify"] == "国家级"]
        df_pivot1.iloc[:,2:]= df_pivot1.iloc[:,2:] * 1
        df_pivot2 =df_pivot.loc[(df_pivot["classify"] == "省市级" )| (df_pivot["classify"] == "省级" )]
        df_pivot2.iloc[:,2:]= df_pivot2.iloc[:,2:] * 0.8
        df_pivot3 =df_pivot.loc[df_pivot["classify"] == "地级"]
        df_pivot3.iloc[:,2:]= df_pivot3.iloc[:,2:] * 0.5
        df_pivot_all = pd.concat([df_pivot1,df_pivot2,df_pivot3],axis=0)
        df_pivot_all = df_pivot_all.drop("classify",axis=1)
        # df_pivot_all = df_pivot_all.drop("山海",axis=1)

        # df_pivot_all.set_index(['found_year'],inplace=True)


        year_list=df_pivot_all["found_year"].tolist()
        year_list =  list(set(year_list))
        df_all=pd.DataFrame()
        df2 = pd.DataFrame()
        df_pivot_cumsum_all = pd.DataFrame()
        now_year = int(datetime.datetime.now().strftime('%Y'))
        for year in year_list:
            if int(year[:4])>= 2016 and int(year[:4])< now_year:
                df_cum1 = df_pivot_all.loc[df_pivot["found_year"] == year]
                df_cum1.set_index(['found_year'], inplace=True)
                df_cumsum = df_cum1.cumsum()
                df_cumsum1 = df_cumsum.iloc[-1,:]
                df_cumsum1 = pd.DataFrame(df_cumsum1 )
                df_cumsum1 .reset_index(drop=False, inplace=True)
                df1 = df_cumsum1
                df1.columns = ["name", 'original_self_value']
                df1['original_self_value']= round(df1['original_self_value'],2)
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
                df2["year"] = year
                df2["original_max_value"] = df1["original_self_value"].max()
                df2["index_name"] = "实验室平台基地数量"
                df2["index_num"] = "2_3"
                df2["index_id"] = "29"
                df2["type"] = "0"
                df2["field"] = field
                df2["value"] = 0.0
                for j in range(len(df2)):
                    if df2["original_self_value"][j] == 0:
                        df2["value"][j] = 0
                    else:
                        df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100,2)

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
                df_all = pd.concat([df2,df_all],axis=0)

        df_all.to_sql('industry_develop_index_ue', con=engine, if_exists='append', index=False)
        print("{}入库成功".format(field))
        print("-" * 50)
    else:
        print("未读取到{}的数据".format(field))
cursor.execute('delete from industry_develop_index where index_num ="2_3"')
print("已删除industry_develop_index中指标值")
sql = '''
insert into industry_develop_index(year,name,value,original_self_value,original_max_value,index_id, index_num,index_name,sequence,type,field)
select year,name,value,original_self_value,original_max_value,index_id,index_num,index_name,sequence,type,field from industry_develop_index_ue where index_num ='2_3'
'''
cursor.execute(sql)
conn.commit()  # 提交，以保存执行结果
print("已将ue数据插入到industry_develop_index")


