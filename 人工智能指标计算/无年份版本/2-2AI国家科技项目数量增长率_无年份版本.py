import sqlalchemy
import pandas as pd
import pymysql

pymysql.install_as_MySQLdb()
import warnings

warnings.filterwarnings('ignore')
import datetime
import time

engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
# engine1 = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')

sql_cmd = '''SELECT
	province,
	YEAR as year,
	count(*) AS num 
FROM
	ai_project_info 
WHERE
	tag = '人工智能' 
GROUP BY
	province,
year'''

df_SQL = pd.read_sql(sql=sql_cmd, con=engine)

df_SQL = df_SQL.loc[df_SQL["province"] != "台湾"]
df_SQL = df_SQL.loc[df_SQL["province"] != "澳门"]
df_SQL = df_SQL.loc[df_SQL["province"] != "香港"]
# df['found_year'] = pd.to_datetime(df['found_year'],format='%Y')
# df["found_year"].astype(str).astype(int)


df_pivot = pd.pivot_table(df_SQL, index="year", columns="province", values="num")
df_pivot = df_pivot.fillna(0)
df_pivot_cumsum = df_pivot.cumsum()
df = df_pivot_cumsum.T

### 将df缺少年份补充
# df_year = list(df.columns)
# now_year = int(datetime.datetime.now().strftime('%Y'))
# list_year = [str(b) for b in range(int(df_year[0]), now_year)]
#
# for s in range(len(list_year)):
#     if list_year[s] not in df_year:
#         df.insert(loc=s, column=list_year[s], value=df.iloc[:, s - 1])

###求每一年的增长率
df_all = pd.DataFrame()
df2 = pd.DataFrame()
df1_1 = pd.DataFrame()
a = df.columns

# min_year = int(a[0])
for i in range(0, df.shape[1]):
    # i=0
    # print(i)
    if i == 0:
        df1 = df.iloc[:, i]
        df1 = pd.DataFrame(df1)
        df1.reset_index(drop=False, inplace=True)
        df1_1["name"]=df1["province"]
        df1_1["original_self_value"] = 0.0
        province_list = ["黑龙江", "辽宁", "吉林", "河北", "河南", "湖北", "湖南", "山东", "山西", "陕西", "安徽", "浙江", "江苏", "福建", \
                         "广东", "海南", "四川", "云南", "贵州", "青海", "甘肃", "江西", "内蒙古", "宁夏", "新疆", "西藏", "广西", "北京", "上海", \
                         "天津", "重庆"]
        notin_list_name = []
        notin_list_value = []
        for b in province_list:
            if b not in df1_1["name"].tolist():
                notin_list_name.append(b)
                notin_list_value.append(0)
        df_not_list = pd.DataFrame({'name': notin_list_name, 'original_self_value': notin_list_value})

        df2 = pd.concat([df1_1, df_not_list], axis=0, ignore_index=True)

        df2["original_max_value"] = 0.0
        df2["value"] = 0.0
        df2["year"] = a[i]
        df2["index_name"] = "国家科技项目数量增长率"
        df2["index_num"] = "2_2"
        df2["index_id"] = "28"
        df2["type"] = "1"
        df2["field"] = "人工智能"
        df2["sequence"] = "0"
        df_all = pd.concat([df2, df_all], axis=0)
    else:
        df1 = df.iloc[:, i]
        df1 = pd.DataFrame(df1)
        df1.reset_index(drop=False, inplace=True)
        df1.columns = ["name", 'now']
        df1_1 = df.iloc[:, i - 1]
        df1_1 = pd.DataFrame(df1_1)
        df1_1.reset_index(drop=False, inplace=True)
        df1["last_year"] = df1_1.iloc[:, 1]

        df1["original_self_value"] = 0.0
        for j in range(len(df1)):
            df1["original_self_value"][j] =  df1["now"][j]-df1["last_year"][j]
            # if df1["now"][j] > 0 and df1["last_year"][j] == 0:
            #     df1["original_self_value"][j] = 100.00
            # elif df1["now"][j] == 0 and df1["last_year"][j] == 0:
            #     df1["original_self_value"][j] = 0.00
            # else:
            #     df1["original_self_value"][j] = round(
            #         ((df1["now"][j] - df1["last_year"][j]) / df1["last_year"][j]) * 100, 2)

        df1_2 = df1[["name", "original_self_value"]]
        province_list = ["黑龙江", "辽宁", "吉林", "河北", "河南", "湖北", "湖南", "山东", "山西", "陕西", "安徽", "浙江", "江苏", "福建", \
                         "广东", "海南", "四川", "云南", "贵州", "青海", "甘肃", "江西", "内蒙古", "宁夏", "新疆", "西藏", "广西", "北京", "上海", \
                         "天津", "重庆"]
        notin_list_name = []
        notin_list_value = []
        for b in province_list:
            if b not in df1_2["name"].tolist():
                notin_list_name.append(b)
                notin_list_value.append(0)
        df_not_list = pd.DataFrame({'name': notin_list_name, 'original_self_value': notin_list_value})

        df2 = pd.concat([df1_2, df_not_list], axis=0, ignore_index=True)




        # df2.reset_index(inplace=True,drop=False)
        df2["year"] = a[i][:4]
        df2["original_max_value"] = df1["original_self_value"].max()
        df2["index_name"] = "国家科技项目数量增长率"
        df2["index_num"] = "2_2"
        df2["index_id"] = "28"
        df2["type"] = "1"
        df2["field"] = "人工智能"
        df2["sequence"] = "0"

        df2["value"] = 0.0
        for j in range(len(df2)):
            if df2["original_self_value"][j] == 0:
                df2["value"][j] = 0
            else:
                df2["value"][j] = round((df2["original_self_value"][j] / df2["original_max_value"][j]) * 100, 1)
        df2.sort_values(by=['value'], ascending=False, inplace=True)
        df_all = pd.concat([df2, df_all], axis=0)

df_all.to_sql('industry_develop_index', con=engine, if_exists='append', index=False)



