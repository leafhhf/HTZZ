import pdfplumber
import os
import json
# from hs_parsing_cwfx import write_to_dict_3, match_cwfx
# from hs_parsing_zyyw import write_to_dict_2, match_zyyw
# from hs_parsing_cbfx import write_to_dict_1, match_cbfx
# from hs_parsing_kggs import write_to_dict_4, match_kggs
# from hs_parsing_gsxx import write_to_dict_5, match_gsxx
from hs_parsing_yftr import write_to_dict_6, match_yftr
from hs2_parsing_yftr5 import write_to_dict_6_2, match_yftr2
# from hs_parsing_kjsj_cwzb import write_to_dict_7, match_kjsj_cwzb
from pymongo import MongoClient
import csv
import datetime
import pandas as pd
# 1.连接本地数据库服务
connection = MongoClient('127.0.0.1')
#"mongodb://localhost:27017/"
# 2.连接本地数据库 demo。没有会创建
db = connection["hadoop"]
# 3.创建集合
mycol = db["hspdf_yanfatouru_2020"]


# 将PDF内容全部提取
def parse_pages(file_path):
    try:
        pages = []
        pdf = pdfplumber.open(file_path)
        print('parse file:{}   page num:{}'.format(os.path.basename(file_path), len(pdf.pages)))
        for index, page in enumerate(pdf.pages):
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
      #  print(0)
        jsondata = dict()
        yftr = []
        yftr.append(dict1)
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
        #print(codes)
        if id in codes:
           # print(1111111111111111)
            b = []
            b.append(dict1)
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
            yftr.append(dict1)
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


def main2(file_dir, result):
    # file_dir PDF的存放路径
    # 保存json的路径

    for root, dirs, files in os.walk(file_dir):
        for index, filename in enumerate(files):

            print("begin......" + filename)

            time1 = []
            time2 = []
            time3 = []

            #time = (filename.split('_')[-1]).split('.')[0]

           # year = time.split('-')[0]
            year='2020'
            time1.append(str(int(year) - 1) + '-12-31')
            time1.append(str(int(year) - 1) + '-1-1')

            time2.append(str(int(year) - 1) + '-12-31')
            time2.append(str(int(year) - 2) + '-12-31')

            time3.append(str(int(year) - 1) + '-12-31')
            time3.append(str(int(year) - 2) + '-12-31')

            root_file = root + '/' + filename
            # print(root_file)
            # company = filename.split('_')[0]
            # stock = filename.split('_')[1]
            # time = (filename.split('_')[-1]).split('.')[0]
            company_name = (filename.split('_')[0]).split('(')[0]

            result_dir = result + filename.split('_')[0] + '-' + filename.split('_')[1].split('.')[0]
            json_name_prefix = filename.split('_')[0] + '-' + filename.split('_')[1].split('.')[0]

            #stock_id = filename.split('_')[0].split('(')[1].split('.')[0]
            stock_id = filename.split('_')[0]

            pages = parse_pages(root_file)

            # try:
            #     zcfz_map, lr_map, xjll_map = match_cwfx(pages)
            #     # list_zyyw = match_zyyw(pages)
            #     dict3 = write_to_dict_3(result_dir, zcfz_map, lr_map, xjll_map, time1, time2, time3)
            #
            #     json_name_cwfx = json_name_prefix + '-' + 'cwfx.json'
            #     # write_to_json(result_dir,json_name_cwfx,dict3)
            #     print("财务分析done......" + filename)
            # except Exception:
            #     print("财务分析解析错误")
            #
            # try:
            #     list_zyyw = match_zyyw(pages)
            #     dict2 = write_to_dict_2(result_dir, list_zyyw)
            #     json_name_zyyw = json_name_prefix + '-' + 'zyyw.json'
            #     # print(dict2)
            #     # write_to_json(result_dir, json_name_zyyw, dict2)
            #     print("主营业务done......" + filename)
            # except Exception:
            #     print("主营业务解析错误")
            #
            # try:
            #     list_cbfx = match_cbfx(pages)
            #     dict1 = write_to_dict_1(result_dir, list_cbfx)
            #     json_name_cbfx = json_name_prefix + '-' + 'cbfx.json'
            #     # write_to_json(result_dir, json_name_cbfx, dict1)
            #     print("成本分析done......" + filename)
            # except Exception:
            #     print("成本分析解析错误")
            #
            # try:
            #     list_kggs = match_kggs(pages)
            #     # list_zyyw = match_zyyw(pages)
            #     dict4 = write_to_dict_4(result_dir, list_kggs)
            #
            #     json_name_kggs = json_name_prefix + '-' + 'kggs.json'
            #     # write_to_json(result_dir,json_name_kggs,dict4)
            #     print("参股控股公司done......" + filename)
            # except Exception:
            #     print("参股控股公司解析错误")
            # try:
            #     gsxx_, lxfs, jbqk, xxpl = match_gsxx(pages)
            #     # list_zyyw = match_zyyw(pages)
            #     dict5 = write_to_dict_5(result_dir, gsxx_, lxfs, jbqk, xxpl, stock_id)
            #
            #     json_name_gsxx = json_name_prefix + '-' + 'gsxx.json'
            #     # write_to_json(result_dir, json_name_gsxx, dict5)
            #     print("公司信息done......" + filename)
            # except Exception:
            #     print("公司信息解析错误")

            try:
                yftr = match_yftr(pages, year)
                # list_zyyw = match_zyyw(pages)
                dict6 = write_to_dict_6(result_dir, yftr)
                if dict6 == {'date': str(int(year) - 1) + '年'}:
                    try:
                        yftr2 = match_yftr2(pages)
                        # list_zyyw = match_zyyw(pages)
                        dict6 = write_to_dict_6_2(result_dir, yftr2, year)
                        print(dict6,'dict6')
                        json_name_yftr2 = json_name_prefix + '-' + 'yftr.json'
                        # write_to_json(result_dir, json_name_yftr2, dict6)
                        print("研发投入done......" + filename)
                        write_to_mongoDB(stock_id, company_name, dict6)
                    except Exception:
                        f1 = open('result/no_yftr.csv', 'a', newline='')
                        f2 = open('result/no_yftr.csv', 'r', newline='')
                        csv_writer = csv.writer(f1)
                        reader = csv.reader(f2)
                        if not [row for row in reader]:
                            csv_writer.writerow(["stock_id", "company_name", "date", "filename", "note"])
                            csv_writer.writerow(
                                [stock_id, company_name, str(int(year) - 1) + '年', filename, '无研发收入或解析错误'])
                        else:
                            csv_writer.writerow(
                                [stock_id, company_name, str(int(year) - 1) + '年', filename, '无研发收入或解析错误'])
                        # f.close()
                        print("研发投入解析错误")
                else:
                    json_name_yftr = json_name_prefix + '-' + 'yftr.json'
                    # write_to_json(result_dir, json_name_yftr, dict6)
                    print("研发投入done......" + filename)
                    write_to_mongoDB(stock_id, company_name, dict6)
                    print('dict6_1',dict6)
            except Exception:
                # print("研发投入解析错误")
                try:
                    yftr2 = match_yftr2(pages)
                    print('yftr2',yftr2)
                    # list_zyyw = match_zyyw(pages)
                    dict6 = write_to_dict_6_2(result_dir, yftr2)
                    #print(dict6,'dict6')
                    json_name_yftr2 = json_name_prefix + '-' + 'yftr.json'
                    # write_to_json(result_dir, json_name_yftr2, dict6)
                    print("研发投入done......" + filename)
                    write_to_mongoDB(stock_id, company_name, dict6)
                    print('dict6_2',dict6)
                except Exception:
                    f1 = open('result/no_yftr.csv', 'a', newline='')
                    f2 = open('result/no_yftr.csv', 'r', newline='')
                    csv_writer = csv.writer(f1)
                    reader = csv.reader(f2)
                    if not [row for row in reader]:
                        csv_writer.writerow(["stock_id", "company_name", "date", "filename", "note"])
                        csv_writer.writerow([stock_id, company_name, str(int(year) - 1) + '年', filename, '无研发收入或解析错误'])
                    else:
                        csv_writer.writerow([stock_id, company_name, str(int(year) - 1) + '年', filename, '无研发收入或解析错误'])
                    # f.close()
                    print("研发投入解析错误")

            # try:
            #     kjsj, cwzb = match_kjsj_cwzb(pages)
            #     print(kjsj)
            #     print(cwzb)
            #     # list_zyyw = match_zyyw(pages)
            #     dict7 = write_to_dict_7(result_dir, kjsj, cwzb)
            #     # print(dict7)
            #     json_name_kjsj_cwzb = json_name_prefix + '-' + 'kjsj_cwzb.json'
            #     # write_to_json(result_dir, json_name_kjsj_cwzb, dict7)
            #     print("主要会计数据和主要财务指标done......" + filename)
            # except Exception:
            #     dict7 = []
            #     print("主要会计数据和主要财务指标解析错误")

            # write_to_mongoDB(stock_id, time, dict1, dict2, dict3, dict4, dict5, dict6, dict7)


if __name__ == '__main__':
    start = datetime.datetime.now()
    print('开始时间：' + str(start))
  #  main2('E://项目汇总//1---getPDF//PDF文件//hs_pdf//GQY视讯(300076.SZ)//年度报告', 'result/')
    #main2('D://天智文档//科技评估中心//北航解析//hs_new//hs//PDF//test', 'resultall/')
    #main2('D:\\software\\python\\FinanceCraw\\hs_pdf', 'resultall/')
    main2(r'E:\沪深和新三板2020年年报\沪深2020年年报', 'resultall/')

    
    end = datetime.datetime.now()
    print('结束时间：' + str(end))
    file = open('result/date.txt', 'a')
    file.write('开始时间：' + str(start) + '\n')
    file.write('结束时间：' + str(end) + '\n')
    print("总共消耗时间为:",end-start)
    file.close()
