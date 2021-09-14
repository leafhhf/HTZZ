import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
import numpy as np

# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
engine2 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/kejibu')
# engine3 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')
cursor = conn.cursor()   #创建游标



field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
for field in field_list:
    #第一步 先读取各个省份企业的雇工人数
    sql_cmd1 ='''select found_year, social_security_staff_num as com_pop,province  from kjb_de_company where label like '%{}%' 
    and not isnull(found_year) and not isnull(social_security_staff_num) and social_security_staff_num != 0 '''.format(field)
    df_SQL = pd.read_sql(sql=sql_cmd1, con=engine)

    if len(df_SQL) > 0:
        print("-" * 50)
        print("读取{}数据成功".format(field))
        print("-" * 50)

        df_SQL["new_province"] = ""
        for i in range(len(df_SQL)):
            if df_SQL["province"][i]:
                if df_SQL["province"][i] != "黑龙江省" and df_SQL["province"][i] != "内蒙古自治区":
                    df_SQL["new_province"][i] = df_SQL["province"][i][:2]
                else:
                    df_SQL["new_province"][i] = df_SQL["province"][i][:3]




        df_SQL1 =  df_SQL[["found_year","new_province","com_pop"]]
        df_SQL1_group = df_SQL1.groupby(["new_province","found_year"]).sum("com_pop")
        df_SQL1_group.reset_index(inplace =True,drop=False)
        df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != ""]
        df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != "台湾"]
        df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != "香港"]
        df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != "澳门"]
        df_SQL1_group.reset_index(inplace =True,drop=True)
        df_SQL1_group['found_year'] = df_SQL1_group['found_year'].astype('int')


        #  筛选初创企业
        now_year = int(datetime.datetime.now().strftime('%Y'))
        list_year = [b for b in range(int(df_SQL1_group['found_year'].min()), now_year)]
        df_SQL1_group_all=pd.DataFrame()
        df_SQL1_group1 = pd.DataFrame()
        for year in list_year:
            # print(year)
            limit_year = year -3
            df_SQL1_group1 = df_SQL1_group.loc[df_SQL1_group['found_year'] <year, :]
            df_SQL1_group1 = df_SQL1_group1.loc[df_SQL1_group1['found_year']>limit_year,:]
            df_SQL1_group1["year"] = year
            df_SQL1_group_all = pd.concat([df_SQL1_group1,df_SQL1_group_all], axis=0, ignore_index=True)



        ## 企业累加
        df_pivot = pd.pivot_table(df_SQL1_group_all, index="year", columns="new_province",values="com_pop",aggfunc=[np.sum])
        df_pivot = df_pivot .fillna(0)
        df_pivot_cumsum = df_pivot.cumsum()
        df = df_pivot_cumsum.T
        df.index=df.index.droplevel()#去掉一个索引

        ## 将df缺少年份补充
        df_year = list(df.columns)
        now_year = int(datetime.datetime.now().strftime('%Y'))
        list_year = [b for b in range(int(df_year[0]), now_year)]

        for s in range(len(list_year)):
            if list_year[s] not in df_year:
                df.insert(loc=s, column=list_year[s], value=df.iloc[:, s - 1])



        ###各个省份的总人数
        sql_cmd2='''
        select year,province,population_num from kejibu.ai_population_num'''
        df_total_pop = pd.read_sql(sql=sql_cmd2, con=engine2)
        print("读取常住人口数据成功")
        print("-" * 50)

        df_total_pop["merge"]=""
        for a in range(len(df_total_pop)):
            df_total_pop["merge"][a] = df_total_pop["year"][a][:4] + df_total_pop["province"][a]
        #封装成字典
        dict1 = dict(zip(df_total_pop['merge'].tolist(),df_total_pop['population_num'].tolist()))



        ###计算指标
        a =df.columns
        df_all=pd.DataFrame()
        df2 = pd.DataFrame()
        now_year = int(datetime.datetime.now().strftime('%Y'))
        for i in range(0, df.shape[1]):
            if int(a[i]) >= 2016 and int(a[i]) < now_year:
            # i=20
                df1 = df.iloc[:,i]
                df1 = pd.DataFrame(df1)
                df1.reset_index(drop=False, inplace=True)
                df1.columns = ["name",'com_pop']
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
                df_not_list = pd.DataFrame({'name': notin_list_name, 'com_pop': notin_list_value})
                df2 = pd.concat([df1, df_not_list], axis=0,ignore_index=True)
                df2["merge"]=""
                for n in range(len(df2["name"])):
                    df2["merge"][n] =str(a[i]) +df2["name"][n]
                df2["total_pop"] = 0
                list1 = df2["merge"].tolist()
                list2 = []
                for m in list1:
                    b = dict1.get(m)
                    list2.append(b)
                df2["original_self_value"]=0.0
                for z in range(len(df2)):
                    df2["total_pop"][z] = list2[z]
                    df2["original_self_value"][z]=round(int(df2["com_pop"][z])/df2["total_pop"][z],2)
                df2["original_max_value"] = df2["original_self_value"].max()

                # df2.reset_index(inplace=True,drop=False)
                df2["year"] =str(a[i]) +"年"
                # df2["original_max_value"] = df1["original_self_value"].max()
                df2["index_name"] = "核心企业人才数量占比"
                df2["index_num"] = "1_8"
                df2["index_id"] = "13"
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


        df_final=df_all[["name","original_self_value","year","original_max_value","index_name","index_num","index_id","type","field","sequence","value"]]
        df_final.to_sql('industry_develop_index_ue', con=engine, if_exists='append', index=False)
        print("{}入库成功".format(field))
        print("-" * 50)
    else:
        print("未读取到{}的数据".format(field))
cursor.execute('delete from industry_develop_index where index_num ="1_8"')
print("已删除industry_develop_index中指标值")
sql = '''
insert into industry_develop_index(year,name,value,original_self_value,original_max_value,index_id, index_num,index_name,sequence,type,field)
select year,name,value,original_self_value,original_max_value,index_id,index_num,index_name,sequence,type,field from industry_develop_index_ue where index_num ='1_8'
'''
cursor.execute(sql)
conn.commit()  # 提交，以保存执行结果
print("已将ue数据插入到industry_develop_index")