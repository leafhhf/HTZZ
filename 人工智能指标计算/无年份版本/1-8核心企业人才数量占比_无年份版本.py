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
engine3 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')

sql_cmd1 ='''select com_registered_name from ai_company_info where industry_all like '%人工智能%' '''
df_aicom = pd.read_sql(sql=sql_cmd1, con=engine)
company_list= df_aicom['com_registered_name']

### 原来9278家人工智能企业能在国信UE数据库匹配上的公司
sql_cmd2 = "SELECT * FROM dm_company WHERE name in ({}) ".format(','.join(["'%s'" % item for item in company_list]))
df_in = pd.read_sql(sql=sql_cmd2, con=engine3)
listed = df_in["name"]
set1=set(listed)
set2=set(company_list)
com_listed=set1 & set2
com_not_listed=set1 ^ set2




sql_cmd3 ='''
	SELECT
		a.NAME,
		estiblish_time,
		social_security_staff_num,
		b.province 
	FROM
	dm_company a
		LEFT JOIN dm_area_code b ON a.area_code = b.area_code 
	WHERE
	NAME IN ({}) 
'''.format(','.join(["'%s'" % item for item in company_list]))
df_SQL = pd.read_sql(sql=sql_cmd3, con=engine3)
# df_test=df_SQL
# df_SQL=df_test
## 清洗数据
df_SQL["year"]=""
for i in range(len(df_SQL)):
    if df_SQL["estiblish_time"][i]:
        df_SQL["year"][i] = df_SQL["estiblish_time"][i][:4]


df_SQL["new_province"] = ""
for i in range(len(df_SQL)):
    if df_SQL["province"][i]:
        if df_SQL["province"][i] != "黑龙江省" and df_SQL["province"][i] != "内蒙古自治区":
            df_SQL["new_province"][i] = df_SQL["province"][i][:2]
        else:
            df_SQL["new_province"][i] = df_SQL["province"][i][:3]
        # elif df_SQL["province"][i] == "黑龙江省" :
        #     df_SQL["new_province"][i] = df_SQL["province"][i][:3]
        # elif df_SQL["province"][i] == "内蒙古自治区":
        #     df_SQL["new_province"][i] = df_SQL["province"][i][:3]
        # elif df_SQL["province"][i] == "内蒙":
        #     df_SQL["new_province"][i] = "内蒙古"



df_SQL1 =  df_SQL[["year","new_province","social_security_staff_num"]]
df_SQL1_group = df_SQL1.groupby(["new_province","year"]).sum("social_security_staff_num")
df_SQL1_group.reset_index(inplace =True,drop=False)
df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != ""]
df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != "台湾"]
df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != "香港"]
df_SQL1_group=df_SQL1_group.loc[df_SQL1_group["new_province"] != "澳门"]
# df_SQL1_group.reset_index(inplace =True)
## 人工智能企业累加
df_pivot = pd.pivot_table(df_SQL1_group, index="year", columns="new_province",values="social_security_staff_num")
df_pivot = df_pivot .fillna(0)
df_pivot_cumsum = df_pivot.cumsum()
df = df_pivot_cumsum.T


###各个省份的总人数
sql_cmd4='''
select year,province,population_num from kejibu.ai_population_num'''
df_total_pop = pd.read_sql(sql=sql_cmd4, con=engine2)

df_total_pop["merge"]=""
for a in range(len(df_total_pop)):
    df_total_pop["merge"][a] = df_total_pop["year"][a][:4] + df_total_pop["province"][a]


a =df_total_pop['merge'].tolist()
b =df_total_pop['population_num'].tolist()
dict1 = dict(zip(a,b))

# df_all["merge"] = ""
# df_all["total_pop"] = ""
# df_all.reset_index(inplace=True,drop=True)
# for i in range(0,len(df_all)):
#     df_all["merge"][i] = df_all["year"][i]+ df_all["name"][i]






###计算指标

a =df.columns
df_all=pd.DataFrame()
df2 = pd.DataFrame()
for i in range(0,df.shape[1]):
    # i=20
    df1 = df.iloc[:,i]
    df1 = pd.DataFrame(df1)
    df1.reset_index(drop=False, inplace=True)
    df1.columns = ["name",'self_pop']
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
    df_not_list = pd.DataFrame({'name': notin_list_name, 'self_pop': notin_list_value})
    df2 = pd.concat([df1, df_not_list], axis=0,ignore_index=True)
    df2["merge"]=""
    for n in range(len(df2["name"])):
        df2["merge"][n] = a[i] +df2["name"][n]
    df2["total_pop"] = 0
    list1 = df2["merge"].tolist()
    list2 = []
    for m in list1:
        b = dict1.get(m)
        list2.append(b)
    df2["original_self_value"]=0.0
    for z in range(len(df2)):
        df2["total_pop"][z] = list2[z]
        df2["original_self_value"][z]=round(int(df2["self_pop"][z])/df2["total_pop"][z],2)
    df2["original_max_value"] = df2["original_self_value"].max()

    # df2.reset_index(inplace=True,drop=False)
    df2["year"] = a[i]+"年"
    # df2["original_max_value"] = df1["original_self_value"].max()
    df2["index_name"] = "核心企业人才数量占比"
    df2["index_num"] = "1_8"
    df2["index_id"] = "13"
    df2["type"] = "0"
    df2["field"] = "人工智能"
    df2["sequence"] = "0"
    df2["value"] = " "
    for j in range(len(df2)):
        if df2["original_self_value"][j] == 0:
            df2["value"][j] = 0
        else:
            df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100,2)

    df2.sort_values(by=['value'], ascending=False, inplace=True)
        # df1["value"][j] = ('%.1f' % df1["value"][j])

    df_all = pd.concat([df2,df_all],axis=0)



df_final=df_all[["name","original_self_value","year","original_max_value","index_name","index_num","index_id","type","field","sequence","value"]]



df_final.to_sql('industry_develop_index', con=engine, if_exists='append', index=False)



