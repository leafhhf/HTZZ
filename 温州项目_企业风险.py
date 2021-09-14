import sqlalchemy
import pandas as pd
import pymysql
import datetime
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')


engine = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/gold_data')
engine1 = sqlalchemy.create_engine('mysql+pymysql://liuye:lyAdmin123@tz!@172.16.16.221:3309/data_tmp')

conn = pymysql.connect(host='172.16.16.221',port=3309,user='liuye',passwd='lyAdmin123@tz!',db='data_tmp')
cursor = conn.cursor()   #创建游标


sql_cmd ='''select cid from b_base_org_list'''
df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
com_list = df_SQL["cid"].tolist()

# -----------------------------------------1、经营风险——动产抵押-----------------------------------------------------
sql_cmd ='''SELECT cid, reg_department as source, publish_date as pub_date, type,amount,term FROM(
SELECT * FROM dm_company_mortgage_info
WHERE cid IN ( {} )  and status = "有效") a  
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险——动产抵押”数据 成功")
    for i in range(len(df_SQL)):
        if df_SQL["pub_date"][i] is None :
            df_SQL["pub_date"][i] = ""
        if df_SQL["term"][i] is None:
            df_SQL["term"][i]= "无"
        if df_SQL["type"][i] is None:
            df_SQL["type"][i] = "无"
        if df_SQL["amount"][i] is None:
            df_SQL["amount"][i] = "无"

    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="动产抵押"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "被担保债权种类为:" + df_SQL["type"][i] +",被担保债权数额为:"+ df_SQL["amount"][i] +"期限为:"+df_SQL["term"][i]
    #入库
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='动产抵押'")
    print("已删除表的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险——动产抵押” 数据")




# -----------------------------------------2、经营风险——动产抵押-变更-----------------------------------------------------
sql_cmd ='''
SELECT a.cid,a.change_date as pub_date,a.change_content as risk_detail FROM(
SELECT * FROM dm_company_mortgage_change 
WHERE cid IN ( {} ) and deleted = 0 ) a   left join dm_company_mortgage_pawn b  on a.main_id =b.main_id where a.deleted = 0

'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险—动产抵押-变更”数据 成功")
    df_SQL["pub_date"] = df_SQL["pub_date"].astype("str")
    df_SQL_final = df_SQL[["cid","pub_date","risk_detail"]]
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="动产抵押-变更"

    #入库
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='动产抵押-变更'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险——动产抵押-变更” 数据")




# -----------------------------------------3、经营风险——动产抵押-抵押人-----------------------------------------------------
sql_cmd ='''
SELECT cid,application_name,license_type FROM dm_company_mortgage_people
WHERE cid IN ( {} ) and deleted = 0 

'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险—动产抵押-抵押人”数据 成功")
    # df_SQL["pub_date"] = df_SQL["pub_date"].astype("str")
    df_SQL["license_type"] = df_SQL["license_type"].astype("str")
    df_SQL_final = df_SQL[["cid"]]
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="动产抵押-抵押人"
    df_SQL_final["risk_detail"] = ""
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "抵押权人名称:" + df_SQL["application_name"][i] +",抵押权人证照/证件类型为："+ df_SQL["license_type"][i]
    #入库
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='动产抵押-抵押人'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险——动产抵押-抵押人” 数据")





# -----------------------------------------4、经营风险——动产抵押 -抵押物-----------------------------------------------------
sql_cmd ='''SELECT cid,pawn_name, detail FROM(
SELECT * FROM dm_company_mortgage_pawn
WHERE cid IN ( {} ) and not isnull(pawn_name) and deleted=0) a  
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险—动产抵押-抵押物”数据 成功")
    df_SQL_final = df_SQL[["cid"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="动产抵押-抵押物"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "抵押物为:" + df_SQL["pawn_name"][i] +",抵押物详细信息为："+ df_SQL["detail"][i]
    #入库
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='动产抵押-抵押物'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)

else:
    print("_" * 50)
    print("无 “经营风险——动产抵押-抵押物” 数据")




# -----------------------------------------5、经营风险——公示催告-----------------------------------------------------
sql_cmd ='''SELECT cid,source_flag as source, gather_name,bill_amt, publish_date as pub_date,end_date FROM(
SELECT * FROM dm_company_public_announcement
WHERE cid IN ( {} ) and deleted = 0) a  
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险—公示催告”数据 成功")
    # df_SQL["pub_date"] = df_SQL["pub_date"].astype("str")
    df_SQL["end_date"] = df_SQL["end_date"].astype("str")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    for i in range(len(df_SQL)):
        if df_SQL["gather_name"][i] is None:
            df_SQL["gather_name"][i] = "无"
        if df_SQL["bill_amt"][i] is None:
            df_SQL["bill_amt"][i] = "无"
        if df_SQL["end_date"][i] is None:
            df_SQL["end_date"][i] = "无"
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="公示催告"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "收款人为:" + df_SQL["gather_name"][i] +",票面金额为："+ df_SQL["bill_amt"][i] +",到期日："+ df_SQL["end_date"][i]
    #入库

    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='公示催告'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)

else:
    print("_" * 50)
    print("无 “经营风险——公示催告” 数据")



# -----------------------------------------6、经营风险——股权出质-----------------------------------------------------
sql_cmd ='''SELECT cid,pledgor,equity_amount, pub_date as pub_date FROM(
SELECT * FROM dm_company_equity_info
WHERE cid IN ( {} ) and deleted = 0 and state != "无效") a  
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险—股权出质”数据 成功")
    df_SQL["pub_date"] = df_SQL["pub_date"].astype("str")
    df_SQL["pub_date"].replace("nan",'',inplace=True)
    df_SQL_final = df_SQL[["cid","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="股权出质"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "出质人为:" + df_SQL["pledgor"][i] +",出质股权数额为："+ df_SQL["equity_amount"][i]
    #入库
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='股权出质'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果

    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)

else:
    print("_" * 50)
    print("无 “经营风险——股权出质” 数据")




# -----------------------------------------7、经营风险——环保处罚-----------------------------------------------------
sql_cmd ='''SELECT cid,publish_time as pub_date ,source_url as source,content,department,reason FROM(
SELECT * FROM dm_company_env_punishment
WHERE cid IN ( {} ) and deleted = 0 ) a  
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险—环保处罚”数据 成功")
    df_SQL["pub_date"] = df_SQL["pub_date"].astype("str")
    df_SQL_final = df_SQL[["cid","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="环保处罚"
    for i in range(len(df_SQL)):
        if df_SQL["department"][i] is None:
            df_SQL["department"][i] = "无"
        if df_SQL["reason"][i] is None:
            df_SQL["reason"][i] = "无"
        if df_SQL["content"][i] is None:
            df_SQL["content"][i] = "无"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "处罚单位为:" + df_SQL["department"][i] +",违法行为为："+ df_SQL["reason"][i]+",执法措施为："+ df_SQL["content"][i]
    #入库
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='环保处罚'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)

else:
    print("_" * 50)
    print("无 “经营风险——环保处罚” 数据")





# -----------------------------------------8、经营风险——简易注销-----------------------------------------------------
sql_cmd ='''
SELECT cid,announcement_term, announcement_end_date,reg_authority FROM dm_company_brief_cancel_announcement
WHERE cid IN ( {} ) and deleted = 0 

'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “经营风险—简易注销”数据 成功")
    df_SQL["announcement_end_date"] = df_SQL["announcement_end_date"].astype("str")
    # df_SQL["license_type"] = df_SQL["license_type"].astype("str")
    df_SQL_final = df_SQL[["cid"]]
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="简易注销"
    df_SQL_final["risk_detail"] = ""
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "公告期:" + df_SQL["announcement_term"][i] +",公告结束日期为："+ df_SQL["announcement_end_date"][i] +",登记机关为："+ df_SQL["reg_authority"][i]
    #入库
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='简易注销'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—简易注销” 数据")





# -----------------------------------------9、经营风险——经营异常-----------------------------------------------------
sql_cmd ='''select cid, put_reason,put_date,put_department,remove_reason,remove_date,
  remove_department from dm_company_abnormal_info where cid IN ( {} ) and deleted= 0
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——经营异常'数据成功")

    df_SQL_final = df_SQL[["cid"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="经营异常"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "列入异常名录原因为:" + df_SQL["put_reason"][i] + "，列入异常名录日期为:" + df_SQL["put_date"][i][:10]  + "，决定列入异常名录部门(作出决定机关)为:" + df_SQL["put_department"][i]  \
                                  + "；移除异常名录原因为:" + df_SQL["put_reason"][i]  + "，移除异常名录日期为:" + df_SQL["put_reason"][i][:10]  + "，决定移除异常名录部门为:" + df_SQL["put_reason"][i]
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='经营异常'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—经营异常” 数据")



# -----------------------------------------10、经营风险——欠税公告-----------------------------------------------------
sql_cmd ='''select cid,own_tax_amount,tax_balance,new_tax_balance,tax_category,source, publish_date as pub_date from (
SELECT * FROM dm_company_own_tax
WHERE cid IN ( {} ) ) a 
where  NOT isnull( own_tax_amount ) 
or NOT isnull( tax_balance ) 
or NOT isnull(new_tax_balance)'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——欠税公告'数据成功")
    df_SQL["own_amount"] = ""
    for i in range(len(df_SQL)):
        if df_SQL["own_tax_amount"][i] is None and df_SQL["tax_balance"][i] is None: #三者中，取有数的那个
                df_SQL["own_amount"][i] = df_SQL["new_tax_balance"][i]
        elif df_SQL["own_tax_amount"][i] is None and df_SQL["new_tax_balance"][i] is None:#三者中，取有数的那个
            df_SQL["own_amount"][i] = df_SQL["tax_balance"][i]
        elif df_SQL["tax_balance"][i] is None and df_SQL["new_tax_balance"][i] is None:#三者中，取有数的那个
            df_SQL["own_amount"][i] = df_SQL["own_tax_amount"][i]
        elif df_SQL["tax_balance"][i] and df_SQL["own_tax_amount"][i] and df_SQL["new_tax_balance"][i]: #三者有数，取new的那个
            df_SQL["own_amount"][i] = df_SQL["new_tax_balance"][i]
        elif df_SQL["tax_balance"][i] and df_SQL["new_tax_balance"][i]:#两者有数，取new的那个
            df_SQL["own_amount"][i] = df_SQL["new_tax_balance"][i]
        elif df_SQL["own_tax_amount"][i] and df_SQL["new_tax_balance"][i]:#两者有数，取new的那个
            df_SQL["own_amount"][i] = df_SQL["new_tax_balance"][i]
        elif df_SQL["own_tax_amount"][i] and df_SQL["tax_balance"][i]:#两者有数，取new的那个
            df_SQL["own_amount"][i] = df_SQL["tax_balance"][i]
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="欠税公告"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "欠" + df_SQL["tax_category"][i] + df_SQL["own_amount"][i] +"元"
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='欠税公告'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险——欠税公告” 数据")





# -----------------------------------------11、经营风险——税收违法-----------------------------------------------------
sql_cmd ='''select cid,case_type, case_info,source,responsible_department,publish_time as pub_date from dm_company_tax_contravention
WHERE cid IN ( {} ) and deleted=0 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——税收违法'数据成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="税收违法"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "案件性质为：" + df_SQL["case_type"][i] + "，违法事实，法律依据，处理处罚情况为：" + df_SQL["case_info"][i]
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='税收违法'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—税收违法” 数据")


# -----------------------------------------12、经营风险——司法拍卖-----------------------------------------------------
# sql_cmd ='''select * from dm_company_judicial_sale where deleted = 0
# '''
# # print(sql_cmd1)
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
#
# #将cids拆开
# list_cid  = []
# list_content = []
# list_pub_time  = []
# list_source_id = []
# for i in range(0, len(df_SQL)):
#     if len(df_SQL["cids"][i]) > 0:
#         a = df_SQL["cids"][i].split(";")
#         for j in a:
#             b = j.strip()
#             list_cid.append(b)
#             list_content.append(df_SQL["introduction"][i])
#             list_pub_time.append(df_SQL["pub_time"][i])
#             list_source_id .append(df_SQL["source_id"][i])
#             a = []
# # 将list转换成dataframe
# df_to_sql = pd.DataFrame({"cid":list_cid,"content":list_content,"pub_time":list_pub_time,"source_id":list_source_id})
#
# #入库再读取
# cursor.execute("truncate table dm_company_judicial_sale_processed")
# conn.commit()  # 提交，以保存执行结果
# df_to_sql.to_sql('dm_company_judicial_sale_processed', con=engine1, if_exists='append', index=False)
# print("入库数据成功")

sql_cmd1 ='''select cid, content as risk_detail, pub_time as pub_date,source_id from  dm_company_judicial_sale_processed WHERE cid IN ( {} )
'''.format(','.join(["'%s'" % item for item in com_list]))
df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine1)

if len(df_SQL1) >0 :
    print("_"*50)
    print("读取'经营风险——司法拍卖'数据成功")
    df_SQL_final = df_SQL1[["cid","risk_detail","pub_date"]]
    df_SQL_final["source"] = ""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="司法拍卖"
    for i in range(len(df_SQL_final)):
        df_SQL_final["source"][i] = "https://sf.taobao.com/notice_detail/" + df_SQL1["source_id"][i] +".htm"
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='司法拍卖'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—司法拍卖” 数据")





# -----------------------------------------13、经营风险——司法拍卖-物品-----------------------------------------------------
# sql_cmd ='''select * from dm_company_judicial_sale_item where deleted = 0
# '''
# # print(sql_cmd1)
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
#
# #将cids拆开
# list_cid  = []
# list_title = []
# list_start_time  = []
# list_consult_price  = []
#
# for i in range(0, len(df_SQL)):
#     if len(df_SQL["cids"][i]) > 0:
#         a = df_SQL["cids"][i].split(";")
#         for j in a:
#             b = j.strip()
#             list_cid.append(b)
#             list_title.append(df_SQL["title"][i])
#             list_start_time.append(df_SQL["start_time"][i])
#             list_consult_price.append(df_SQL["consult_price"][i])
#             a = []
#
# # 将list转换成dataframe
# df_to_sql = pd.DataFrame({"cid":list_cid,"title":list_title,"start_time":list_start_time,"consult_price":list_consult_price})
#
# #入库再读取
# cursor.execute("truncate table dm_company_judicial_sale_item_processed")
# conn.commit()  # 提交，以保存执行结果
# df_to_sql.to_sql('dm_company_judicial_sale_item_processed', con=engine, if_exists='append', index=False)
# print("入库数据成功")

sql_cmd1 ='''select cid,title, start_time as pub_date,consult_price from  dm_company_judicial_sale_item_processed WHERE cid IN ( {} )
'''.format(','.join(["'%s'" % item for item in com_list]))
df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine1)


if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——司法拍卖-物品'数据成功")
    df_SQL_final = df_SQL1[["cid", "pub_date"]]
    df_SQL_final["risk_detail"] = ""
    df_SQL_final["dalei_risk"] = "经营风险"
    df_SQL_final["xiaolei_risk"] = "司法拍卖-物品"
    df_SQL1["consult_price"] = df_SQL1["consult_price"].astype("str")
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "拍卖竞标物品为：" + df_SQL1["title"][i]+"，竞标物品评估价为：" + df_SQL1["consult_price"][i] +"元"
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='司法拍卖-物品'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—司法拍卖-物品” 数据")






# -----------------------------------------14、经营风险——土地抵押-----------------------------------------------------
sql_cmd ='''select  mortgagor_cid as cid,mortgagor,mortgagee,area,source_url as source,start_date,end_date,mortgage_amount from dm_company_land_mortgage
WHERE mortgagor_cid IN ( {}) and deleted=0 


'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——土地抵押'数据成功")
    df_SQL["start_date"] = df_SQL["start_date"].astype("str")
    df_SQL["start_date"] = df_SQL["end_date"].astype("str")
    df_SQL_final = df_SQL[["cid","source"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="土地抵押"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "土地抵押权人,即拥有抵押后的土地为：" + df_SQL["mortgagee"][i] + "，抵押面积(公顷)为：" + df_SQL["area"][i] \
                                    + "，抵押土地的金额(万元)为：" + df_SQL["mortgage_amount"][i]+"，土地抵押登记起始时间为：" + str(df_SQL["start_date"][i])  \
                                    +"，土地抵押登记结束时间：" + str(df_SQL["end_date"][i])
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='土地抵押'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—土地抵押” 数据")





# -----------------------------------------15、经营风险——行政处罚-----------------------------------------------------
sql_cmd ='''select cid,type,content,publish_date as pub_date,desc_file_path as source from dm_company_punishment_info
WHERE cid IN ( {} ) and deleted=0 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——行政处罚'数据成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="行政处罚"
    df_SQL["pub_date"]= df_SQL["pub_date"].astype("str")
    for i in range(len(df_SQL)):
        if df_SQL["type"][i] is None:
            df_SQL["type"][i] = "无"
        if df_SQL["content"][i] is None:
            df_SQL["content"][i] = "无"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "处罚原因为：" + df_SQL["type"][i] + "，行政处罚内容为：" + df_SQL["content"][i]
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='行政处罚'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—行政处罚” 数据")






# -----------------------------------------16、经营风险——行政处罚-信用中国-----------------------------------------------------
sql_cmd ='''select cid,punish_name,result,reason,decision_date as pub_date,source  from dm_company_punishment_info_creditchina
WHERE cid IN ( {} ) and deleted=0 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——行政处罚-信用中国'数据成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="行政处罚-信用中国"
    for i in range(len(df_SQL)):
        if df_SQL["punish_name"][i] is None:
            df_SQL["punish_name"][i] = "无"
        if df_SQL["result"][i] is None:
            df_SQL["result"][i] = "无"
        if df_SQL["reason"][i] is None:
            df_SQL["reason"][i] = "无"

    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "处罚名称为：" + df_SQL["punish_name"][i] + "，处罚结果为：" + df_SQL["result"][i]+ "，处罚事由为：" + df_SQL["reason"][i]
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='行政处罚-信用中国'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—行政处罚-信用中国” 数据")





# -----------------------------------------17、经营风险——询价评估-----------------------------------------------------
sql_cmd ='''select cid,secondary_type, subject_name,source,insert_time as pub_date from dm_company_zxr_evaluate
WHERE cid IN ( {}) and deleted=0 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——询价评估'数据成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="询价评估"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "财产类型为：" + df_SQL["secondary_type"][i] + "，标的物名称为：" + df_SQL["subject_name"][i]
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='询价评估'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—询价评估” 数据")







# -----------------------------------------18、经营风险——严重违法-----------------------------------------------------
sql_cmd ='''select cid,put_reason,put_date,put_department,remove_reason,remove_date,remove_department,type,fact from dm_company_illegal_info
WHERE cid IN ( {}) and deleted=0 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——严重违法'数据成功")
    df_SQL_final = df_SQL[["cid"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="严重违法"
    for i in range(len(df_SQL_final)):
        if df_SQL["remove_date"][i] is None:
            df_SQL_final["risk_detail"][i] = "类别为：" + df_SQL["type"][i] + ";列入原因为：" + df_SQL["put_reason"][i] + "，列入日期为：" + df_SQL["put_date"][i] \
                                             + "，决定列入部门(作出决定机关)为：" + df_SQL["put_department"][i] +",现仍未被移除"
        else:
            df_SQL_final["risk"][i] = "类别为：" + df_SQL["type"][i] + ";列入原因为：" + df_SQL["put_reason"][i] + "，列入日期为：" + df_SQL["put_date"][i] \
                                      + "，决定列入部门(作出决定机关)为：" + df_SQL["put_department"][i] + "；移除日期'为：" + df_SQL["remove_date"][i] + "，移除原因为：" + df_SQL["remove_reason"][i] \
                                      + "，决定移除部门为：" + df_SQL["remove_department"][i]

    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='严重违法'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—严重违法” 数据")



# -----------------------------------------19、经营风险——知识产权出质-----------------------------------------------------
sql_cmd ='''select cid,ipr_name,pledgee_name, ipr_type,pledge_reg_period ,pub_date as pub_date from dm_company_ipr_pledge
WHERE cid IN ( {}) and deleted=0 and status = "有效"
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
if len(df_SQL) >0 :
    print("_"*50)
    print("读取'经营风险——知识产权出质'数据成功")
    df_SQL_final = df_SQL[["cid","pub_date"]]
    df_SQL_final["risk_detail"]=""
    df_SQL_final["dalei_risk"]="经营风险"
    df_SQL_final["xiaolei_risk"]="知识产权出质"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "知识产权出质名称为：" + df_SQL["ipr_name"][i] + "，知识产权出质种类为：" + df_SQL["ipr_type"][i]+\
                                  "，质权人为：" + df_SQL["pledgee_name"][i] + "，质权登记期限为：" + df_SQL["pledge_reg_period"][i]
    #入库
    #先将原来的风险内容删除再新增进去
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '经营风险' and xiaolei_risk ='知识产权出质'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “经营风险—知识产权出质” 数据")





# -----------------------------------------20、司法风险-被执行人-----------------------------------------------------
sql_cmd ='''
SELECT cids as cid,case_create_time as pub_date,exec_money,source,court FROM dm_company_zxr
WHERE cids IN ( {} ) 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)

if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “司法风险——被执行人”数据 成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["dalei_risk"]="司法风险"
    df_SQL_final["xiaolei_risk"]="被执行人"
    df_SQL_final["risk_detail"]=""
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "执行法院为：" + df_SQL["court"][i] + "，执行标的金额为：" + df_SQL["exec_money"][i]
    # 入库执行标的是你欠别人的钱数，未履行金额是0
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='被执行人'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “司法风险——被执行人” 数据")






# -----------------------------------------21、司法风险-裁判文书-----------------------------------------------------
# sql_cmd ='''select * from dm_company_lawsuit where deleted = 0
# '''
# # print(sql_cmd1)
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
#
# #将cids拆开
# list_cid  = []
# list_plaintiff_cids = []
# list_defendant_cids  = []
# list_url  = []
# list_doc_type  = []
# list_publish_time  = []
# list_title  = []
# for i in range(0, len(df_SQL)):
#     if len(df_SQL["cids"][i]) > 0:
#         a = df_SQL["cids"][i].split(";")
#         for j in a:
#             b = j.strip()
#             list_cid.append(b)
#             list_plaintiff_cids.append(df_SQL["plaintiff_cids"][i])
#             list_defendant_cids.append(df_SQL["defendant_cids"][i])
#             list_url.append(df_SQL["url"][i])
#             list_doc_type .append(df_SQL["doc_type"][i])
#             list_publish_time.append(df_SQL["publish_time"][i])
#             list_title.append(df_SQL["title"][i])
#             a = []
#
# # 将list转换成dataframe
# df_to_sql = pd.DataFrame({"cid":list_cid,"plaintiff_cids":list_plaintiff_cids,"defendant_cids":list_defendant_cids,"url":list_url,
#                           "doc_type":list_doc_type,"publish_time":list_publish_time,"title":list_title})
#
# #入库再读取
# cursor.execute("truncate table kjb_company_lawsuit_processed")
# conn.commit()  # 提交，以保存执行结果
# df_to_sql.to_sql('kjb_company_lawsuit_processed', con=engine, if_exists='append', index=False)
# print("入库数据成功")


# sql_cmd ='''
# SELECT cid,plaintiff_cids ,defendant_cids,url,doc_type,publish_time,title FROM kjb_company_lawsuit_processed
# WHERE cid IN ( {} )
# '''.format(','.join(["'%s'" % item for item in com_list]))
# # print(sql_cmd1)
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
#
# if len(df_SQL) >0 :
#     print("_"*50)
#     print("读取 “司法风险——裁判文书”数据 成功")
#     df_SQL_final = df_SQL[["cid","pub_date"]]
#     df_SQL_final["dalei_risk"]="司法风险"
#     df_SQL_final["xiaolei_risk"]="裁判文书"
#     df_SQL_final["risk"]=""
#     for i in range(len(df_SQL_final)):
#         df_SQL_final["risk"][i] = "欠" + df_SQL["exec_amount"][i] + "元"+","+"未履行金额为:"+df_SQL["no_exec_amount"][i] +"元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
#
#     cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='裁判文书'")
#     print("已删除中wenzhou_com_risk的数据")
#     conn.commit()  # 提交，以保存执行结果
#     df_SQL_final.to_sql('wenzhou_com_risk', con=engine, if_exists='append', index=False)
# else:
#     print("_" * 50)
#     print("无 “司法风险——裁判文书” 数据")





# # -----------------------------------------22、司法风险-法院公告-----------------------------------------------------
# sql_cmd ='''select * from dm_company_court_announcement where deleted = 0
# '''
# # print(sql_cmd1)
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
#
# #将cids拆开
# list_cid  = []
# list_plaintiff_cids = []
# list_defendant_cids  = []
# list_url  = []
# list_doc_type  = []
# list_publish_time  = []
# list_title  = []
# for i in range(0, len(df_SQL)):
#     if len(df_SQL["cids"][i]) > 0:
#         a = df_SQL["cids"][i].split(";")
#         for j in a:
#             b = j.strip()
#             list_cid.append(b)
#             list_plaintiff_cids.append(df_SQL["plaintiff_cids"][i])
#             list_defendant_cids.append(df_SQL["defendant_cids"][i])
#             list_url.append(df_SQL["url"][i])
#             list_doc_type .append(df_SQL["doc_type"][i])
#             list_publish_time.append(df_SQL["publish_time"][i])
#             list_title.append(df_SQL["title"][i])
#             a = []
#
# # 将list转换成dataframe
# df_to_sql = pd.DataFrame({"cid":list_cid,"plaintiff_cids":list_plaintiff_cids,"defendant_cids":list_defendant_cids,"url":list_url,
#                           "doc_type":list_doc_type,"publish_time":list_publish_time,"title":list_title})
#
# #入库再读取
# cursor.execute("truncate table kjb_company_lawsuit_processed")
# conn.commit()  # 提交，以保存执行结果
# df_to_sql.to_sql('kjb_company_lawsuit_processed', con=engine, if_exists='append', index=False)
# print("入库数据成功")
#
#
# if len(df_SQL) >0 :
#     print("_"*50)
#     print("读取 “司法风险——法院公告”数据 成功")
#     df_SQL_final = df_SQL[["cid","source","pub_date"]]
#     df_SQL_final["dalei_risk"]="司法风险"
#     df_SQL_final["xiaolei_risk"]="法院公告"
#     df_SQL_final["risk"]=""
#     for i in range(len(df_SQL_final)):
#         if df_SQL["no_exec_amount"][i] is None:
#             df_SQL["no_exec_amount"][i]="0"
#             df_SQL_final["risk"][i] = "欠" + df_SQL["exec_amount"][i] + "元"+","+"未履行金额为:"+df_SQL["no_exec_amount"][i] +"元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
#         else:
#             df_SQL_final["risk"][i] = "欠" + df_SQL["exec_amount"][i] + "元" + "," + "未履行金额为:" + df_SQL["no_exec_amount"][i] + "元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
#     # 入库执行标的是你欠别人的钱数，未履行金额是0
#     cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='法院公告'")
#     print("已删除中wenzhou_com_risk的数据")
#     conn.commit()  # 提交，以保存执行结果
#     df_SQL_final.to_sql('wenzhou_com_risk', con=engine, if_exists='append', index=False)
# else:
#     print("_" * 50)
#     print("无 “司法风险——法院公告” 数据")
#
#
# # -----------------------------------------23、司法风险-开庭公告-----------------------------------------------------
# sql_cmd ='''SELECT cid,court_name as source, case_create_time as pub_date, case_final_time,no_exec_amount,exec_amount FROM(
# SELECT * FROM dm_company_zxr_final_case
# WHERE cid IN ( {} ) ) a
# where NOT isnull(exec_amount ) and exec_amount != 0
# '''.format(','.join(["'%s'" % item for item in com_list]))
# # print(sql_cmd1)
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
#
# if len(df_SQL) >0 :
#     print("_"*50)
#     print("读取 “司法风险——开庭公告”数据 成功")
#     df_SQL_final = df_SQL[["cid","source","pub_date"]]
#     df_SQL_final["dalei_risk"]="司法风险"
#     df_SQL_final["xiaolei_risk"]="开庭公告"
#     df_SQL_final["risk"]=""
#     for i in range(len(df_SQL_final)):
#         if df_SQL["no_exec_amount"][i] is None:
#             df_SQL["no_exec_amount"][i]="0"
#             df_SQL_final["risk"][i] = "欠" + df_SQL["exec_amount"][i] + "元"+","+"未履行金额为:"+df_SQL["no_exec_amount"][i] +"元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
#         else:
#             df_SQL_final["risk"][i] = "欠" + df_SQL["exec_amount"][i] + "元" + "," + "未履行金额为:" + df_SQL["no_exec_amount"][i] + "元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
#     # 入库执行标的是你欠别人的钱数，未履行金额是0
#     cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='开庭公告'")
#     print("已删除中wenzhou_com_risk的数据")
#     conn.commit()  # 提交，以保存执行结果
#     df_SQL_final.to_sql('wenzhou_com_risk', con=engine, if_exists='append', index=False)
# else:
#     print("_" * 50)
#     print("无 “司法风险——开庭公告” 数据")
#
#
#
#
#
# -----------------------------------------24、司法风险-立案信息-----------------------------------------------------
# sql_cmd ='''select * from company_court_register where deleted = 0
# '''
#
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
#
# #将cids拆开
# list_cid  = []
# list_filing_date = []
# list_content = []
# list_source_url  = []
# list_plaintiff  = []
# list_defendant  = []
# list_case_type  = []
#
# for i in range(0, len(df_SQL)):
#     if len(df_SQL["cids"][i]) > 0:
#         a = df_SQL["cids"][i].split(";")
#         for j in a:
#             b = j.strip()
#             list_cid.append(b)
#             list_filing_date.append(df_SQL["filing_date"][i])
#             list_content.append(df_SQL["content"][i])
#             list_source_url.append(df_SQL["source_url"][i])
#             list_plaintiff.append(df_SQL["plaintiff"][i])
#             list_defendant.append(df_SQL["defendant"][i])
#             list_case_type.append(df_SQL["case_type"][i])
#             a = []
#
# # 将list转换成dataframe
# df_to_sql = pd.DataFrame({"cid":list_cid,"filing_date":list_filing_date,"content":list_content,"source_url":list_source_url,
#                           "plaintiff":list_plaintiff,"defendant":list_defendant,"case_type":list_case_type})
#
# #入库再读取
# cursor.execute("truncate table company_court_register_processed")
# conn.commit()  # 提交，以保存执行结果
# df_to_sql.to_sql('company_court_register_processed', con=engine, if_exists='append', index=False)
# print("入库数据成功")



sql_cmd1 ='''
SELECT cid, filing_date as pub_date, source_url as source,plaintiff,defendant,case_type FROM company_court_register_processed
WHERE cid IN ( {} )
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine1)

if len(df_SQL1) >0 :
    print("_"*50)
    print("读取 “司法风险——立案信息”数据 成功")
    df_SQL_final = df_SQL1[["cid","source","pub_date"]]
    df_SQL_final["dalei_risk"]="司法风险"
    df_SQL_final["xiaolei_risk"]="立案信息"
    df_SQL_final["risk_detail"]=""
    for i in range(len(df_SQL_final)):
       df_SQL_final["risk_detail"][i] = "公诉人/原告/上诉人/申请人为："+df_SQL1["plaintiff"][i] + "，被告人/被告/被上诉人/被申请人为：" + df_SQL1["defendant"][i]

    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='立案信息'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “司法风险——立案信息” 数据")


# -----------------------------------------25、司法风险-失信人-----------------------------------------------------
sql_cmd ='''
SELECT cid,name, duty,performance, pub_date as pub_date,lawsuit_url as source FROM dm_company_dishonest_info
WHERE cid IN ( {} ) 

'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)

if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “司法风险——失信人”数据 成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["dalei_risk"]="司法风险"
    df_SQL_final["xiaolei_risk"]="失信人"
    df_SQL_final["risk_detail"]=""
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "失信人为：" + df_SQL["name"][i] + "，生效法律文书确定的义务：" + df_SQL["duty"][i] + "履行情况为："+df_SQL["performance"]

    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='失信人'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “司法风险——失信人” 数据")


# -----------------------------------------26、司法风险-司法协助-----------------------------------------------------
# sql_cmd ='''
# SELECT * FROM dm_company_judicial_assistance
# WHERE cid IN ( {} ) and deleted =0
#
# '''.format(','.join(["'%s'" % item for item in com_list]))
# # print(sql_cmd1)
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine)
#
# if len(df_SQL) >0 :
#     print("_"*50)
#     print("读取 “司法风险——司法协助”数据 成功")
#     df_SQL_final = df_SQL[["cid","source","report_time"]]
#     df_SQL_final["dalei_risk"]="司法风险"
#     df_SQL_final["xiaolei_risk"]="司法协助"
#     df_SQL_final["risk_detail"]=""
#     for i in range(len(df_SQL_final)):
#         if df_SQL["no_exec_amount"][i] is None:
#             df_SQL["no_exec_amount"][i]="0"
#             df_SQL_final["risk_detail"][i] = "欠" + df_SQL["exec_amount"][i] + "元"+","+"未履行金额为:"+df_SQL["no_exec_amount"][i] +"元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
#         else:
#             df_SQL_final["risk_detail"][i] = "欠" + df_SQL["exec_amount"][i] + "元" + "," + "未履行金额为:" + df_SQL["no_exec_amount"][i] + "元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
#     # 入库执行标的是你欠别人的钱数，未履行金额是0
#     cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='司法协助'")
#     print("已删除中wenzhou_com_risk的数据")
#     conn.commit()  # 提交，以保存执行结果
#     df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
# else:
#     print("_" * 50)
#     print("无 “司法风险——司法协助” 数据")





# -----------------------------------------27、司法风险-送达公告-----------------------------------------------------
# sql_cmd ='''select * from dm_company_send_announcement where deleted = 0
# '''
#
# df_SQL = pd.read_sql(sql=sql_cmd, con=engine1)
#
# #将cids拆开
# list_cid  = []
# list_plaintiff = []
# list_case_reason = []
# list_source_url  = []
# list_start_date  = []
# list_content  = []
# df_SQL["defendant_cids"].fillna("",inplace=True)
#
# for i in range(0, len(df_SQL)):
#     if len(df_SQL["defendant_cids"][i]) > 0:
#         a = df_SQL["defendant_cids"][i].split(";")
#         for j in a:
#             b = j.strip()
#             list_cid.append(b)
#             list_plaintiff.append(df_SQL["plaintiff"][i])
#             list_case_reason.append(df_SQL["case_reason"][i])
#             list_source_url.append(df_SQL["source_url"][i])
#             list_content.append(df_SQL["content"][i])
#             list_start_date.append(df_SQL["start_date"][i])
#             a = []
#
# # 将list转换成dataframe
# df_to_sql = pd.DataFrame({"cid":list_cid,"plaintiff":list_plaintiff,"case_reason":list_case_reason,"source_url":list_source_url,"content":list_content,
#                           "start_date":list_start_date})
#
# #入库再读取
# cursor.execute("truncate table kjb_company_send_announcement_processed")
# conn.commit()  # 提交，以保存执行结果
# df_to_sql.to_sql('kjb_company_send_announcement_processed', con=engine, if_exists='append', index=False)
# print("入库数据成功")


sql_cmd1 ='''
SELECT cid, start_date as pub_date, source_url as source,plaintiff,case_reason,content FROM kjb_company_send_announcement_processed
WHERE cid IN ( {} ) 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL1 = pd.read_sql(sql=sql_cmd1, con=engine1)

if len(df_SQL1) >0 :
    print("_"*50)
    print("读取 “司法风险——送达公告”数据 成功")
    df_SQL_final = df_SQL1[["cid","source","pub_date"]]
    df_SQL_final["dalei_risk"]="司法风险"
    df_SQL_final["xiaolei_risk"]="送达公告"
    df_SQL_final["risk_detail"]=""
    df_SQL1["content"] = df_SQL1["content"].astype("str")
    df_SQL1["case_reason"] = df_SQL1["case_reason"].astype("str")
    df_SQL1["plaintiff"] = df_SQL1["plaintiff"].astype("str")

    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "原告/上诉人为：" + df_SQL1["plaintiff"][i] +"，案由为：" + df_SQL1["case_reason"][i]+"，公告内容为：" + df_SQL1["content"][i]

    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='送达公告'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “司法风险——送达公告” 数据")


# -----------------------------------------28、司法风险-限制消费令-----------------------------------------------------
sql_cmd ='''
SELECT cid,name,court_name,case_create_time as pub_date ,content,file_path as source FROM dm_company_zxr_restrict
WHERE cid IN ( {} ) and deleted = 0 
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)

if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “司法风险——限制消费令”数据 成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["dalei_risk"]="司法风险"
    df_SQL_final["xiaolei_risk"]="限制消费令"
    df_SQL_final["risk_detail"]=""
    for i in range(len(df_SQL)):
        if df_SQL["name"][i] is None:
            df_SQL["name"][i] = "无"
        if df_SQL["court_name"][i] is None:
            df_SQL["court_name"][i] = "无"
        if df_SQL["content"][i] is None:
            df_SQL["content"][i] = "无"
    for i in range(len(df_SQL_final)):
        df_SQL_final["risk_detail"][i] = "公司人员：" + df_SQL["name"][i] + "被限制消费，" + "执行法院名称为:" + df_SQL["court_name"][i] + "限制消费令正文为：" +  df_SQL["content"][i]

    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='限制消费令'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “司法风险——限制消费令” 数据")





# -----------------------------------------29、司法风险-终本案件-----------------------------------------------------
sql_cmd ='''SELECT cid,court_name as source, case_create_time as pub_date, case_final_time,no_exec_amount,exec_amount FROM(
SELECT * FROM dm_company_zxr_final_case
WHERE cid IN ( {} ) ) a 
where NOT isnull(exec_amount ) and exec_amount != 0
'''.format(','.join(["'%s'" % item for item in com_list]))
# print(sql_cmd1)
df_SQL = pd.read_sql(sql=sql_cmd, con=engine)

if len(df_SQL) >0 :
    print("_"*50)
    print("读取 “司法风险——终本案件”数据 成功")
    df_SQL_final = df_SQL[["cid","source","pub_date"]]
    df_SQL_final["dalei_risk"]="司法风险"
    df_SQL_final["xiaolei_risk"]="终本案件"
    df_SQL_final["risk_detail"]=""
    for i in range(len(df_SQL_final)):
        if df_SQL["no_exec_amount"][i] is None:
            df_SQL["no_exec_amount"][i]="0"
            df_SQL_final["risk_detail"][i] = "欠" + df_SQL["exec_amount"][i] + "元"+","+"未履行金额为:"+df_SQL["no_exec_amount"][i] +"元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
        else:
            df_SQL_final["risk_detail"][i] = "欠" + df_SQL["exec_amount"][i] + "元" + "," + "未履行金额为:" + df_SQL["no_exec_amount"][i] + "元"+","+"于"+df_SQL["case_final_time"][i][:10]+"终结本次执行程序"
    # 入库执行标的是你欠别人的钱数，未履行金额是0
    cursor.execute("delete from wenzhou_com_risk where dalei_risk = '司法风险' and xiaolei_risk ='终本案件'")
    print("已删除中wenzhou_com_risk的数据")
    conn.commit()  # 提交，以保存执行结果
    df_SQL_final.to_sql('wenzhou_com_risk', con=engine1, if_exists='append', index=False)
else:
    print("_" * 50)
    print("无 “司法风险——终本案件” 数据")















