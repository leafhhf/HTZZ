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

# conn = pymysql.connect(host='172.16.16.100',port=3306,user='root',passwd='tianzhi',db='rawdata')
# cursor = conn.cursor()


#
# com = pd.read_excel(r"C:\Users\Administrator\Desktop\python_work\经信局\car_beijing.xlsx",sheet_name="终版")
# company_list = com["com_name"].unique().tolist()

field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
similarity_list = ['similarity_2','similarity_9','similarity_10','similarity_7','similarity_4','similarity_3',
                   'similarity_6','similarity_11','similarity_5','vague_similarity']
for f in range(len(field_list)):
    sql_cmd1 = '''select name,social_security_staff_num,{} from kjb_de_company where label like "%{}%" and  not isnull(name) and  not isnull(social_security_staff_num)'''.format(similarity_list[f],field_list[f])
    df_SQL1 =  pd.read_sql(sql=sql_cmd1, con=engine)
    # df_SQL1 = df_SQL1.loc[df_SQL1["reg_capital"] != ""]
    # df_SQL1.reset_index(inplace= True,drop = True)
    print("读取{}数据成功".format(field_list[f]))
    # df_SQL1["social_security_staff_num"] = df_SQL1["cleaned_reg_capital"].astype("float")
    df_SQL1["{}".format(similarity_list[f])] = df_SQL1["{}".format(similarity_list[f])].astype("float")
    df_SQL1["original_self_value"] = 0.0
    for i in range(len(df_SQL1)):
        print(i)
        df_SQL1["original_self_value"][i] = round(df_SQL1["social_security_staff_num"][i] * df_SQL1["{}".format(similarity_list[f])][i],2)


    df_SQL1.sort_values("original_self_value",ascending=False,inplace=True)
    df_SQL1.reset_index(inplace =True,drop=True)
    df_SQL1["field"] = field_list[f]


    # df_pivot = pd.pivot_table(df_SQL1, index="name", columns="last_point",values="value")
    # df_pivot = df_pivot.fillna(0)
    df = df_SQL1[["name","original_self_value","field"]]
    df.columns = ["com","original_self_value","field"]

    df["original_max_value"] = df["original_self_value"].max()
    df["original_self_value"] = round(df["original_self_value"],2)
    df["index_name"] = "企业员工人数"
    df["index_num"] = "1_2"

    df["value"] = ""
    df["value"] = df["original_self_value"].rank(method="min", ascending=False)

    df.to_sql('kjb_longtou_com', con=engine, if_exists='append', index=False)
    print("{}入库成功".format(field_list[f]))


# df.to_excel("经信局_注册资本指标计算结果.xlsx")