
### excel文件合并成csv
# import pandas as pd
# import os
# filepath = r"E:\11111-工作内容\AAA-科技部\全球上市公司数据\原表"
# filenames = os.listdir(filepath)
#
# # print(filenames)
# num = 0
#
# dfs = []
# for name in filenames:
#     print(f'第{num}个文件正在合并中')
# #     print(path +"\\"+name)
#     dfs.append(pd.read_excel(os.path.join(filepath,name),sheet_name="列表"))
#     num += 1    #为了查看合并到第几个表格了
#
#
# df = pd.concat(dfs)
#
# df.to_csv(r"E:\11111-工作内容\AAA-科技部\全球上市公司数据\原表\all.csv",  index=False, header=False)
#
# #writer=pd.ExcelWriter(r"E:\11111-工作内容\AAA-科技部\全球上市公司数据\原表\combine.csv")
# #df.to_csv(writer,sheet_name='Data1',index=False)
#
# #writer.save()
#
# print('done')



#---入库的原始数据处理再入终表库
import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings('ignore')

# 读取BVD原始数据表的sql的数据
engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/rawdata')
sql_cmd ='''select * from bvd_g20_listed_company_info_original'''

df = pd.read_sql(sql=sql_cmd, con=engine)

#  将n.a.值替换一下
# df = df.replace("n.a.","")


# 不同年份的数据
df1= df[['company_name','field','country','yanfatouru2020','rongzi2020','yingyeshouru2020',"bvd_index_num",'found_date']]
# df1 = df1.replace("0.0","")
df1.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df1["year"] = 2020

df2=df[['company_name','field','country','yanfatouru2019','rongzi2019','yingyeshouru2019',"bvd_index_num",'found_date']]
# df2 = df2.replace("0.0","")
df2.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df2["year"] = 2019
#df2.head()

df3= df[['company_name','field','country','yanfatouru2018','rongzi2018','yingyeshouru2018',"bvd_index_num",'found_date']]
# df3 = df3.replace("0.0","")
df3.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df3["year"] = 2018
#df3.head()

df4= df[['company_name','field','country','yanfatouru2017','rongzi2017','yingyeshouru2017',"bvd_index_num",'found_date']]
# df4 = df4.replace("0.0","")
df4.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df4["year"] = 2017


df5= df[['company_name','field','country','yanfatouru2016','rongzi2016','yingyeshouru2016',"bvd_index_num",'found_date']]
# df5 = df5.replace("0.0","")
df5.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df5["year"] = 2016


df6= df[['company_name','field','country','yanfatouru2015','rongzi2015','yingyeshouru2015',"bvd_index_num",'found_date']]
# df6 = df6.replace("0.0","")
df6.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df6["year"] = 2015


df7= df[['company_name','field','country','yanfatouru2014','rongzi2014','yingyeshouru2014',"bvd_index_num",'found_date']]
# df7 = df7.replace("0.0","")
df7.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df7["year"] = 2014


df8= df[['company_name','field','country','yanfatouru2013','rongzi2013','yingyeshouru2013',"bvd_index_num",'found_date']]
# df8 = df8.replace("0.0","")
df8.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df8["year"] = 2013


df9= df[['company_name','field','country','yanfatouru2012','rongzi2012','yingyeshouru2012',"bvd_index_num",'found_date']]
# df9 = df9.replace("0.0","")
df9.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df9["year"] = 2012


df10= df[['company_name','field','country','yanfatouru2011','rongzi2011','yingyeshouru2011',"bvd_index_num",'found_date']]
# df10 = df10.replace("0.0","")
df10.columns=[['company_name','field','country','yanfatouru','rongzi','yingyeshouru','bvd_index_num','found_date']]
df10["year"] = 2011





# 拼接
df_final = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10])
df_final.reset_index(inplace=True,drop=True)



#导出再读吧，索引有报错
df_final.to_csv("./bvd数据处理/all_processes_data0809.csv",index=0)



data=pd.read_csv("./bvd数据处理/all_processes_data0809.csv",encoding='utf8')
# data.drop(["Unnamed: 0"],axis=1,inplace=True)

# 一起处理一下时间格式
data.sort_values(by=["company_name","year"],inplace=True,ascending=False)
data['found_date'] = pd.to_datetime(data['found_date'], errors='coerce')
#data['found_date'] = data['found_date'].apply(lambda x: pd.to_datetime(x).strftime('%m/%d/%Y')[0])
data['found_year']=pd.to_datetime(data['found_date'], format='%m/%d/%Y').dt.year


# 将公司成立时间小于财报数据时间的行删掉
data1 = data.loc[ ~ (data["year"] < data["found_year"])]


data1.to_csv("./bvd数据处理/final_tabledata0809.csv",index=0)
#data1=pd.read_csv(r'E:\11111-工作内容\AAA-科技部\全球上市公司数据\final_tabledata.csv',encoding='utf8')

#入库,数据量太大，还是kettle入库吧
#engine = sqlalchemy.create_engine('mysql+pymysql://root:tianzhi@172.16.16.100:3306/ly')

# connection = pymysql.connect(
#     host='172.16.16.100',
#     port=3306,
#     user='root',
#     password='tianzhi',
#     db='ly')
# #sql_cmd ='''select * from bvd_listed_company_info_original'''
# try:
#     # 获取会话指针
#     with connection.cursor() as cursor:
#         # 创建sql语句
#         sql = "insert into bvd_listed_company_info_final(company_name,country,yanfatouru,rongzi,yingyeshouru,found_date,year) values (%s,%s,%s,%s,%s,%s,%s)"
#
#         # 执行sql
#         cursor.execute(sql, (df_final["company_name"], df_final["country"], df_final["yanfatouru"],df_final["rongzi"],df_final["yingyeshouru"],df_final["found_date"],df_final["year"]))
#         # cursor.execute(sql)
#         # 提交
#         connection.commit()
# except:
#     print("something wrong")
# finally:
#     connection.close()
# df = pd.read_sql(sql=sql_cmd, con=engine)





























