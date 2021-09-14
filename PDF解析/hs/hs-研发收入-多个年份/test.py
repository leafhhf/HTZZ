import pdfplumber
import os
import json
# from hs_parsing_cwfx import write_to_dict_3, match_cwfx
# from hs_parsing_zyyw import write_to_dict_2, match_zyyw
# from hs_parsing_cbfx import write_to_dict_1, match_cbfx
# from hs_parsing_kggs import write_to_dict_4, match_kggs
# from hs_parsing_gsxx import write_to_dict_5, match_gsxx
#from hs_parsing_yftr import write_to_dict_6, match_yftr
from hs2_parsing_yftr5 import write_to_dict_6_2, match_yftr2
# from hs_parsing_kjsj_cwzb import write_to_dict_7, match_kjsj_cwzb
from pymongo import MongoClient
import csv
import datetime

# 1.连接本地数据库服务
connection = MongoClient('172.16.16.97')
# 2.连接本地数据库 demo。没有会创建
db = connection["hadoop"]
# 3.创建集合
mycol = db["hushen_yanfatouru_bc"]


# 将PDF内容全部提取
def parse_pages(file_path):
    try:
        pages = []
        pdf = pdfplumber.open(file_path)
        print('parse file:{}   page num:{}'.format(os.path.basename(file_path), len(pdf.pages)))
        for index, page in enumerate(pdf.pages):
            # print(index)
            if index == 50:
                break
            tables = page.extract_tables()
            text = page.extract_text()

            if len(tables) < 1:
                continue
            pages.append({'text': text, 'tables': tables, 'page': index + 1})
        return pages
    except Exception as e:
        print(e)
    return None


def write_to_json(result_dir, json_name, dict):
    with open(result_dir + '/' + json_name, 'w', encoding='utf-8') as f:
        json.dump(dict, f, indent=4, ensure_ascii=False)


# 判断key是否为空
def key_kong(jsondata):
    for k, v in jsondata.items():
        if k == '':
            jsondata = dict()
            break
        if type(v) == dict:
            key_kong(v)
    return jsondata


def write_to_mongoDB(id, company_name, dict1):
    # collist = db.list_collection_names()
    # print(collist)
    if mycol.find().count() == 0:
        print(0)
        jsondata = dict()
        yftr = []
        yftr.extend(dict1)
        # dict4 = key_kong(dict4)
        jsondata['code'] = id
        jsondata['gsmc'] = company_name
        # jsondata['data'] = time
        # jsondata['gsxx'] = dict5
        # jsondata['kggs'] = dict4
        # jsondata['cwfx'] = dict3
        # jsondata['zyyw'] = dict2
        # jsondata['cbfx'] = dict1
        jsondata['yftr'] = yftr
        # jsondata['kjsj_cwzb'] = dict7
        mycol.insert(jsondata)
        print("写入数据库成功")
    else:
        codes = []
        for item in mycol.find():
            codes.append(item['code'])
        print(codes)
        if id in codes:
            print(1111111111111111)
            b = []
            b.extend(dict1)
            # if type(item['yftr']) == dict():
            #     print(222)
            #     b.append(item['yftr'])
            # else:
            #     print(33333)
            #     b.extend(item['yftr'])
            for item in mycol.find():
                if item['code'] == id:
                    b.extend(item['yftr'])
                    print(b)
                    mycol.update_one({"code": id}, {'$set': {"yftr": b}})
                    print("写入数据库成功")
                else:
                    continue
        else:
            jsondata = dict()
            yftr = []
            yftr.extend(dict1)
            # dict4 = key_kong(dict4)
            jsondata['code'] = id
            jsondata['gsmc'] = company_name
            # jsondata['data'] = time
            # jsondata['gsxx'] = dict5
            # jsondata['kggs'] = dict4
            # jsondata['cwfx'] = dict3
            # jsondata['zyyw'] = dict2
            # jsondata['cbfx'] = dict1
            jsondata['yftr'] = yftr
            # jsondata['kjsj_cwzb'] = dict7
            print("#######################")
            mycol.insert(jsondata)
            print("写入数据库成功")


# 获得年报路径
def get_nianbao(file_dir):
    '''
    g = os.walk(file_dir)
    print('ggggggggg',g)
    for path, dir_list, file_list in g:
        for dir_name in dir_list:
            for p1, d1, f1 in os.walk(file_dir + "/" + dir_name):
                print('dir_list',dir_list)
                for d in d1:
                    if d == '年度报告':
                        print(file_dir + "/" + dir_name + "/" + d)
                        dir = file_dir + "/" + dir_name + "/" + d
                        main2(dir, 'result/')
                        # return file_dir + "/" + dir_name + "/" + d
    '''
   
    with open('one.txt',"r") as f:
        lines = f.readlines()
    print('linesssssssss',lines)
    
    for i in lines:
        root_file = i.replace("\n", "")
        print('dddddd',root_file)
       # file_list=file_dir

        dir = file_dir + "/" + root_file + "/" + '年度报告'
        print('aaaaaa',dir)
                #main2(dir, 'result/')
        #return dir


if __name__ == '__main__':
    start = datetime.datetime.now()
    print('开始时间：' + str(start))
    dir = get_nianbao('E:\\项目汇总\\1---getPDF\\PDF文件\\hs_pdf')
   # print('dir',dir)
    #main2(dir, 'result/')
    end = datetime.datetime.now()
    print('结束时间：' + str(end))
    file = open('result/date.txt', 'a')
    file.write('开始时间：' + str(start) + '\n')
    file.write('结束时间：' + str(end) + '\n')
    file.close()
