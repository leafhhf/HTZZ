import sqlalchemy
import pandas as pd
import pymysql
import datetime

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings('ignore')
import numpy as np
import copy

# 读取sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
#
# com = pd.read_excel(r"C:\Users\Administrator\Desktop\python_work\经信局\car_beijing.xlsx",sheet_name="终版")
# company_list = com["com_name"].unique().tolist()



# 获取各领域的公司名单
sql_cmd1 = '''select reg_capital from kjb_de_company'''
df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine)
# df_SQL1 = df_SQL1.loc[df_SQL1["reg_capital"] != ""]
# df_SQL1.reset_index(inplace=True, drop=True)

##清洗注册资本数据
df_SQL1["cleaned_reg_capital"] = ""
for i in range(23621,len(df_SQL1)):
    print(i)
    if df_SQL1["reg_capital"][i]:
        df_SQL1["reg_capital"][i] = df_SQL1["reg_capital"][i].replace(" ", "")
        df_SQL1["reg_capital"][i] = df_SQL1["reg_capital"][i].replace(",", "")
        if df_SQL1["reg_capital"][i].find("万人民币元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万人民币元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("(万元人民币)") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("(万元人民币)", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("(美元)") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("(美元)", "")
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i].replace("万元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 6.5

        elif df_SQL1["reg_capital"][i].find("万美元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万美元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 6.5
        elif df_SQL1["reg_capital"][i].find("万元美元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万元美元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 6.5
        elif df_SQL1["reg_capital"][i].find("(万人民币)") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("(万人民币)", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("万人民币") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万人民币", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("(人民币)") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("(人民币)", "")
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i].replace("万元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("(万元)") >= 0:
            df_SQL1["reg_capital"][i] = df_SQL1["reg_capital"][i].replace(",", "")
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("(万元)", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("万元人民币") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万元人民币", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("（单位：元/万元）") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("（单位：元/万元）", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("元/万元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("元/万元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("万欧元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万欧元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 7.6
        elif df_SQL1["reg_capital"][i].find("万香港元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万香港元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 0.89
        elif df_SQL1["reg_capital"][i].find("万元港元（港币）") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万元港元（港币）", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i]  * 0.89
        elif df_SQL1["reg_capital"][i].find("万元港币") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万元港币", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i]  * 0.89
        elif df_SQL1["reg_capital"][i].find("万港元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万港元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i]  * 0.89
        elif df_SQL1["reg_capital"][i].find("万港币") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万港币", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i]  * 0.89
        elif df_SQL1["reg_capital"][i].find("(香港元)") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("(香港元)", "")
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i].replace("万元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 0.89
        elif df_SQL1["reg_capital"][i].find("(港币)") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("(港币)", "")
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i].replace("万元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 0.89
        elif df_SQL1["reg_capital"][i].find("万\xa0元人民币") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万\xa0元人民币", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("万日元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万日元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] / 17
        elif df_SQL1["reg_capital"][i].find("万瑞士法郎") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万瑞士法郎", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] * 7.03
        elif df_SQL1["reg_capital"][i].find("万澳大利亚元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万澳大利亚元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["cleaned_reg_capital"][i] *  4.76
        elif df_SQL1["reg_capital"][i].find("万元") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万元", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])
        elif df_SQL1["reg_capital"][i].find("万") >= 0:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital"][i].replace("万", "")
            df_SQL1["cleaned_reg_capital"][i] = float(df_SQL1["cleaned_reg_capital"][i])