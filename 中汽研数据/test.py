import pandas as pd

df1 = pd.read_excel("C:/Users/Administrator/Desktop/python_work/中汽研数据/4 企业算法数据5000.xlsx")
list_us= df1["所属公司"].tolist()
set1=set(list_us)
df2 = pd.read_excel("C:/Users/Administrator/Desktop/python_work/中汽研数据/汽车产业链分析（北京温州襄阳）--6.21.xlsx",
                    sheet_name='北京详细数据',header=1)
df3 = pd.read_excel("C:/Users/Administrator/Desktop/python_work/中汽研数据/汽车产业链分析（北京温州襄阳）--6.21.xlsx",
                    sheet_name='温州详细数据',header=1)
df4 = pd.read_excel("C:/Users/Administrator/Desktop/python_work/中汽研数据/汽车产业链分析（北京温州襄阳）--6.21.xlsx",
                    sheet_name='襄阳详细数据',header=1)


df5 = df2["企业"].dropna().tolist()
df6 = df3["企业"].dropna().tolist()
df7 = df4["企业"].dropna().tolist()


list_zqy=[]

for i in df5:
    list_zqy.append(i)


for i in df6:
    list_zqy.append(i)

for i in df7:
    list_zqy.append(i)


set1=set(list_us)
set2=set(list_zqy)

same = set1 & set2
diff= set1 ^ set2



