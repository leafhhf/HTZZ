import pymysql
conn=pymysql.connect('172.16.16.152','root','123456','kejibu',charset='utf8')
cur=conn.cursor()
import json
#data={"_id":{"$oid":"5e0f1670364aad722254f734"},"code":"300076","gsmc":"GQY视讯","yftr":[{"date":"2016年","研发人员数量（人）":"64","研发人员数量占比":"18.23%","研发投入金额（元）":"19,889,018.26","研发投入占营业收入比例":"11.56%","研发支出资本化的金额（元）":"8,580,390.10","资本化研发支出占研发投入":"43.14%","的比例":"","资本化研发支出占当期净利润的比重":"-39.55%"},{"date":"2015年","研发人员数量（人）":"60","研发人员数量占比":"18.58%","研发投入金额（元）":"11,869,507.11","研发投入占营业收入比例":"5.83%","研发支出资本化的金额（元）":"3,155,585.54","资本化研发支出占研发投入的比例":"26.59%","资本化研发支出占当期净利润的比重":"64.41%"}]}
datalist=[]
with open('D:\天智文档\科技评估中心\北航解析\研发投入json\yftr.csv','r',encoding='utf-8') as f:
    for line in f:
        datalist.append(json.loads(line))
for dict in datalist:
  #  print('dict',dict)
    gsmc = dict['gsmc']
    code = dict['code']
    yftr = dict[u'yftr']
    print()
    j=0
    for i in yftr:
        #dict_new = {value:key for key,value in i.items()}
        
       # print(isinstance(i,list))
        
        if isinstance(i,list):
           # print('aa',range(len(i)))
            for j in range(len(i)):
               # print('bb',j,i[j])
                a=i[j]
                date = a['date']
                for value,key in a.items():
                    if value !='date':
                        sql="insert into hushen_yftr_2020(gsmc,code,date1,value1,key1) values('%s','%s','%s','%s','%s');" %(gsmc,code,date,value,key)
                        cur.execute(sql)
                    conn.commit()

        else:
            date = i['date']
        #print('######',dict_new)
            for value,key in i.items():
                if value !='date':
                   # print(gsmc,code,date,value,key)
                    sql="insert into hushen_yftr_2020(gsmc,code,date1,value1,key1) values('%s','%s','%s','%s','%s');" %(gsmc,code,date,value,key)
               # print(sql)
                    cur.execute(sql)
       # print('i',i)
    #print(yftr)
                conn.commit()
conn.close()
