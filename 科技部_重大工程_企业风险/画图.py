import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import os

plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

data = pd.read_excel("C:/Users/Administrator/Desktop/经济规模可视化.xlsx",sheet_name=2,header=0,index_col=None)
# path = 'C:/Users/Administrator/Desktop'
# excelFile = os.path.join(path,'经济规模可视化.xlsx')
# data = pd.read_excel(excelFile,sheet_name=2)

a = data["2020年增速"].values
b = data["2020年占GDP比重"].values
c = data["2020年经济规模"].values
# province = data["省份"].loc[(data["省份"]!="东北") &(data["省份"]!="华东") &(data["省份"]!="华南")
#                             &(data["省份"]!="华北")&(data["省份"]!="华中")&(data["省份"]!="西北")
#                           &(data["省份"]!="西南") ].values
province = data["省份"].values
N=31
fig=plt.figure(figsize=(5,5))
area = np.pi * c/6 ** 2
plt.xlim(xmax=0.35,xmin=-0.05)
plt.ylim(ymax=0.175,ymin=0.02)
plt.xlabel('数字经济占GDP比重')  # 横坐标轴标题
plt.ylabel('数字经济规模增速')  # 纵坐标轴标题
plt.title("我国部分省市数字经济规模、占比、增速",fontsize=12)
plt.scatter(a, b, s=area*1.5, c=c,
            cmap=mpl.cm. RdYlGn_r, marker="o",alpha=0.5)
for i in range(N):
    if province[i] == "上海":
        plt.annotate(province[i],xy=(a[i],b[i]))
    else:
        plt.annotate(province[i],xy=(a[i],b[i]))
plt.show()