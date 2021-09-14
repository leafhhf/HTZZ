import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

# 读取sql的数据
# engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
# # engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
engine2 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/data_tmp')
# conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='knowledge_graph')

df = pd.read_excel(r"E:\11111-工作内容\吴总发\智能网联\all.xls")
df = df.fillna("无")


df["last_point"] = ""

for i in range(len(df)):
    if df["六级节点"][i] == "无" and df["五级节点"][i] == "无" and df["四级节点"][i] == "无" and df["三级节点"][i] == "无" and df["二级节点"][i] != "无":
        df["last_point"][i] = df["二级节点"][i]
    elif df["六级节点"][i] == "无" and df["五级节点"][i] == "无" and df["四级节点"][i] == "无" and df["三级节点"][i] != "无":
        df["last_point"][i] = df["三级节点"][i]
    elif df["六级节点"][i] == "无" and df["五级节点"][i] == "无" and df["四级节点"][i] != "无":
        df["last_point"][i] = df["四级节点"][i]
    elif df["六级节点"][i] == "无" and df["五级节点"][i] != "无" :
        df["last_point"][i] = df["五级节点"][i]
    elif df["六级节点"][i] != "无":
        df["last_point"][i] = df["六级节点"][i]

df_final = df[['province','公开号','技术领域','last_point','一级节点','二级节点', '三级节点', '四级节点', '五级节点', '六级节点']]
# df_final.rename({"公开号":,"技术领域":"industry","一级节点":"first_point","二级节点":"second_point","三级节点":"third_point",
#                  "四级节点":"fourth_point","五级节点":"fifth_point","六级节点":"sixth_point"},inplace = True)


df_final.columns = ['province',"pub_number","industry",'last_point',"first_point","second_point","third_point","fourth_point","fifth_point","sixth_point"]
df_final.to_sql('jxj_icvs_patent_v1', con=engine2, if_exists='append', index=False)



df_final.to_excel("test.xlsx")



