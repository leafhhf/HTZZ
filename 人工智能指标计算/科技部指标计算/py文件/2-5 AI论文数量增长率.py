import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')

engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd = '''   SELECT province,pub_year FROM ai_journal_article_managed  
 where country like '%中国%' 
 and field like '%人工智能%' 
 and pub_year <= "2020" '''
df = pd.read_sql(sql=sql_cmd, con=engine)
df.head()

# df.to_csv('test_all.csv')

list_province = []
list_year =[]
i = 0
for i in range(len(df)):
    if len(df["province"][i]) > 0:
        a = df["province"][i].split(",")
        for j in a:
            b = j.strip()
            list_province.append(b)
            list_year.append(df["pub_year"][i])
            i += 1
            a = []

# 将list转换成dataframe  并将list转换成dataframe
df1 = pd.DataFrame({'province': list_province,'year':list_year})
df1 = df1.loc[ df1["province"] != ""]
df1 = df1.loc[ df1["province"] != "香港"]
df1 = df1.loc[ df1["province"] != "台湾"]


# df1.rename(columns={0: "省份"}, inplace=True)

# 2019年
df1_2019 = df1.loc[df1["year"] <= "2019"].groupby(by = "province").count().reset_index()
df1_2019.rename(columns={"province": "省份","year":"2019数量"}, inplace=True)
df1_2020 = df1.loc[df1["year"] <= "2020"].groupby(by = "province").count().reset_index()
df1_2020.rename(columns={"province": "省份","year":"2020数量"}, inplace=True)
df_19_and_20 = pd.merge(df1_2019,df1_2020,on="省份")


df_19_and_20["增速"] = " "
for i in range(len(df_19_and_20)):
    df_19_and_20["增速"][i] = ((df_19_and_20["2020数量"][i] -df_19_and_20["2019数量"][i])/ df_19_and_20["2019数量"][i])



df_19_and_20["最大值"] = df_19_and_20["增速"].max()
df_19_and_20.sort_values(by="增速",inplace=True,ascending=False)
df_19_and_20.reset_index(inplace=True,drop=True)

df_19_and_20["指标值"] = " "
for i in range(len(df_19_and_20)):
    df_19_and_20["指标值"][i] = (df_19_and_20["增速"][i] / df_19_and_20["最大值"][i]) * 100
    df_19_and_20["指标值"][i] = ('%.1f' % df_19_and_20["指标值"][i])



print(df_19_and_20)
