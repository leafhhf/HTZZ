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

engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/knowledge_graph')
field_list = ["人工智能","5G","互联网","物联网","大数据","区块链","智能制造","智能芯片","云计算","数字经济"]
similarity_list = ['similarity_2','similarity_9','similarity_10','similarity_7','similarity_4','similarity_3',
                   'similarity_6','similarity_11','similarity_5','vague_similarity']
for f in range(len(field_list)):
    sql_cmd1 = '''select name,reg_capital,reg_capital_amount,reg_capital_currency,{} from kjb_de_company where label like "%{}%" and  not isnull(name) and  not isnull(reg_capital) and not isnull(reg_capital_amount)'''.format(similarity_list[f],field_list[f])
    df_SQL1 =  pd.read_sql(sql=sql_cmd1, con=engine)
    print("读取{}数据成功".format(field_list[f]))
    df_SQL1 = df_SQL1.loc[df_SQL1["reg_capital"] != ""]
    df_SQL1.reset_index(inplace= True,drop = True)


    df_SQL1["cleaned_reg_capital"] = ""
    for i in range(len(df_SQL1)):
        print(i)
        if df_SQL1["reg_capital_currency"][i]=="港元":
            df_SQL1["cleaned_reg_capital"][i]= df_SQL1["reg_capital_amount"][i] *0.829 /10000
        elif df_SQL1["reg_capital_currency"][i]=="人民币":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i]/10000
        elif df_SQL1["reg_capital_currency"][i]=="美元":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *6.446/10000
        elif df_SQL1["reg_capital_currency"][i]=="欧元":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *7.6256/10000
        elif df_SQL1["reg_capital_currency"][i]=="日元":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *0.05866/10000
        elif df_SQL1["reg_capital_currency"][i]=="瑞士法郎":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *7.0327/10000
        elif df_SQL1["reg_capital_currency"][i]=="港币":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i]*0.829/10000
        elif df_SQL1["reg_capital_currency"][i]=="澳大利亚元":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *4.7623/10000
        elif df_SQL1["reg_capital_currency"][i]=="新加坡元":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *4.8101/10000
        elif df_SQL1["reg_capital_currency"][i]=="加元":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *5.0985/10000
        elif df_SQL1["reg_capital_currency"][i]=="法国法郎":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *7.6256/10000
        elif df_SQL1["reg_capital_currency"][i]=="瑞典克朗":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i] *1.3341/10000
        elif df_SQL1["reg_capital_currency"][i]=="英镑":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i]* 8.9284/10000
        elif df_SQL1["reg_capital_currency"][i]=="德国马克":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i]*7.6256/10000
        elif df_SQL1["reg_capital_currency"][i]=="加拿大元":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i]*5.0985/10000
        elif df_SQL1["reg_capital_currency"][i] is None:
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i]/10000
        elif df_SQL1["reg_capital_currency"][i] =="":
            df_SQL1["cleaned_reg_capital"][i] = df_SQL1["reg_capital_amount"][i]/10000

    df_SQL1["cleaned_reg_capital"] = df_SQL1["cleaned_reg_capital"].astype("float")
    df_SQL1["{}".format(similarity_list[f])] = df_SQL1["{}".format(similarity_list[f])].astype("float")
    df_SQL1["zhuceziben"] = 0.0
    for i in range(len(df_SQL1)):
        print(i)
        df_SQL1["zhuceziben"][i] = df_SQL1["cleaned_reg_capital"][i] * df_SQL1["{}".format(similarity_list[f])][i]
    df_SQL1.sort_values("zhuceziben",ascending=False,inplace=True)
    df_SQL1.reset_index(inplace =True,drop=True)
    df_SQL1["field"] = field_list[f]


    # df_pivot = pd.pivot_table(df_SQL1, index="name", columns="last_point",values="value")
    # df_pivot = df_pivot.fillna(0)
    df = df_SQL1[["name","zhuceziben","field"]]
    df.columns = ["com","original_self_value","field"]

    df["original_max_value"] = df["original_self_value"].max()
    df["original_self_value"] = round(df["original_self_value"],2)
    df["index_name"] = "企业当前注册资本规模"
    df["index_num"] = "1_1"



    df["value"] = ""
    df["value"] = df["original_self_value"].rank(method="min",ascending=False)
    # df["value"] = 0.0
    # for j in range(len(df)):
    #     print(j)
    #     if df["original_self_value"][j] == 0:
    #         df["value"][j] = 0
    #     else:
    #         df["value"][j] = round((df["original_self_value"][j] / df["original_max_value"][j]) * 100, 2)

    df.to_sql('kjb_longtou_com', con=engine, if_exists='append', index=False)
    print("{}入库成功".format(field_list[f]))

# df.to_excel("经信局_注册资本指标计算结果.xlsx")