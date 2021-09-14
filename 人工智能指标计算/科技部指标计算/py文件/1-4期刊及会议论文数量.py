import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')

engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd = '''select province from ai_journal_article_managed where country like "%中国%" and pub_year<='2020' and field like '%人工智能%' '''
df = pd.read_sql(sql=sql_cmd, con=engine)
df.head()

list_province = []
i = 0
for i in range(len(df)):
    if len(df["province"][i]) > 0:
        a = df["province"][i].split(",")
        for j in a:
            b = j.strip()
            list_province.append(b)
            i += 1
            a = []

# 将list转换成dataframe
df1 = pd.DataFrame(data=list_province)
df1.rename(columns={0: "省份"}, inplace=True)
df1_count = df1.value_counts().sort_values(ascending=False)
df2 = pd.DataFrame(data= df1_count,columns=["期刊及会议论文数量"])

df2["最大值"] = df2["期刊及会议论文数量"].max()
df2["指标值"] = " "
for i in range(len(df2)):
    df2["指标值"][i] = (df2["期刊及会议论文数量"][i] / df2["最大值"][i]) * 100
    df2["指标值"][i] = ('%.1f' % df2["指标值"][i])

# 把年份信息插入
list_year = []
i = 0
while True:
    if i < len(df2):
        list_year.append("2020年")
        i += 1
    else:
        break
df2.insert(loc=0, column="年份", value=list_year)
print(df2)
df2.reset_index(level="省份",inplace=True)


df2 = df2.loc[df2["省份"] != ""]  # 去除空白的省份
df2 = df2.loc[df2["省份"] != "台湾"]  # 保留中国大陆省份
df2 = df2.loc[df2["省份"] != "台湾"]
df2 = df2.loc[df2["省份"] != "台湾"]

print(df2)
