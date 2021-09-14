# coding: utf-8
import pdfplumber
import os
import json
from pymongo import MongoClient

# 1.连接本地数据库服务
connection = MongoClient('localhost')
# 2.连接本地数据库 demo。没有会创建
db = connection.demo06
# 3.创建集合
emp = db.hsxx

a = {'本期费用化研发投入': '47,555,213', '本期资本化研发投入': '-', '研发投入合计': '46,555,213', '研发投入总额占营业收入比例（%）': '0.23', '公司研发人员的数量': '76', '研发人员数量占公司总人数的比例（%）': '1.42', '研发投入资本化的比重（%）': '-'}


for item in db.hsxx.find():
    # pass
    # print(item)
    b = []
    b.append(a)
    if item['code'] == '600597':
        print(item)
        b.append(item['yftr'])
        db.hsxx.update_one({"code":"600597"},{'$set':{"yftr":b}})

for item in db.hsxx.find():
    # pass
    # print(item)
    if item['code'] == '600597':
        print(item)
        # db.hsxx.update_one({"code":"600597"},{'$set':{"yftr":a}})


# print(db.hsxx.find())
