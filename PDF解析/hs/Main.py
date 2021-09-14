import pdfplumber
import os
import json
from hs_parsing_cwfx import write_to_dict_3, match_cwfx
from hs_parsing_zyyw import write_to_dict_2, match_zyyw
from hs_parsing_cbfx import write_to_dict_1, match_cbfx
from hs_parsing_kggs import write_to_dict_4, match_kggs
from hs_parsing_gsxx import write_to_dict_5, match_gsxx
from hs_parsing_yftr import write_to_dict_6, match_yftr
from hs_parsing_kjsj_cwzb import write_to_dict_7, match_kjsj_cwzb
from pymongo import MongoClient

# 1.连接本地数据库服务
connection = MongoClient('localhost')
# 2.连接本地数据库 demo。没有会创建
db = connection.demo06
# 3.创建集合
emp = db.hsxx


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


def write_to_mongoDB(id, time, dict1):

    jsondata = dict()
    # dict4 = key_kong(dict4)
    jsondata['code'] = id
    # # jsondata['data'] = time
    # jsondata['gsxx'] = dict5
    # jsondata['kggs'] = dict4
    # jsondata['cwfx'] = dict3
    # jsondata['zyyw'] = dict2
    # jsondata['cbfx'] = dict1
    jsondata['yftr'] = dict1
    # jsondata['kjsj_cwzb'] = dict7

    print("#######################")
    emp.insert(jsondata)
    print("写入数据库成功")


def main(file_dir, result):
    # file_dir PDF的存放路径
    # 保存json的路径

    for root, dirs, files in os.walk(file_dir):
        for index, filename in enumerate(files):

            print("begin......" + filename)

            time1 = []
            time2 = []
            time3 = []

            time = (filename.split('_')[-1]).split('.')[0]

            year = time.split('-')[0]
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

            result_dir = result + filename.split('_')[0] + '-' + filename.split('_')[1].split('.')[0]
            json_name_prefix = filename.split('_')[0] + '-' + filename.split('_')[1].split('.')[0]

            stock_id = filename.split('_')[0].split('(')[1].split('.')[0]

            pages = parse_pages(root_file)

            try:
                zcfz_map, lr_map, xjll_map = match_cwfx(pages)
                # list_zyyw = match_zyyw(pages)
                dict3 = write_to_dict_3(result_dir, zcfz_map, lr_map, xjll_map, time1, time2, time3)

                json_name_cwfx = json_name_prefix + '-' + 'cwfx.json'
                write_to_json(result_dir, json_name_cwfx, dict3)
                print("财务分析done......" + filename)
            except Exception:
                print("财务分析解析错误")

            try:
                list_zyyw = match_zyyw(pages)
                dict2 = write_to_dict_2(result_dir, list_zyyw)
                json_name_zyyw = json_name_prefix + '-' + 'zyyw.json'
                # print(dict2)
                write_to_json(result_dir, json_name_zyyw, dict2)
                print("主营业务done......" + filename)
            except Exception:
                print("主营业务解析错误")

            try:
                list_cbfx = match_cbfx(pages)
                dict1 = write_to_dict_1(result_dir, list_cbfx)
                json_name_cbfx = json_name_prefix + '-' + 'cbfx.json'
                write_to_json(result_dir, json_name_cbfx, dict1)
                print("成本分析done......" + filename)
            except Exception:
                print("成本分析解析错误")

            try:
                list_kggs = match_kggs(pages)
                # list_zyyw = match_zyyw(pages)
                dict4 = write_to_dict_4(result_dir, list_kggs)

                json_name_kggs = json_name_prefix + '-' + 'kggs.json'
                write_to_json(result_dir, json_name_kggs, dict4)
                print("参股控股公司done......" + filename)
            except Exception:
                print("参股控股公司解析错误")
            try:
                gsxx_, lxfs, jbqk, xxpl = match_gsxx(pages)
                # list_zyyw = match_zyyw(pages)
                dict5 = write_to_dict_5(result_dir, gsxx_, lxfs, jbqk, xxpl, stock_id)

                json_name_gsxx = json_name_prefix + '-' + 'gsxx.json'
                write_to_json(result_dir, json_name_gsxx, dict5)
                print("公司信息done......" + filename)
            except Exception:
                print("公司信息解析错误")

            try:
                yftr = match_yftr(pages)
                # list_zyyw = match_zyyw(pages)
                dict6 = write_to_dict_6(result_dir, yftr)

                json_name_yftr = json_name_prefix + '-' + 'yftr.json'
                write_to_json(result_dir, json_name_yftr, dict6)
                print("研发投入done......" + filename)
            except Exception:
                print("研发投入解析错误")


            try:
                kjsj, cwzb = match_kjsj_cwzb(pages)
                # list_zyyw = match_zyyw(pages)
                dict7 = write_to_dict_7(result_dir, kjsj, cwzb)

                json_name_kjsj_cwzb = json_name_prefix + '-' + 'kjsj_cwzb.json'
                write_to_json(result_dir, json_name_kjsj_cwzb, dict7)
                print("主要会计数据和主要财务指标done......" + filename)
            except Exception:
                print("主要会计数据和主要财务指标解析错误")


def main2(file_dir, result):
    # file_dir PDF的存放路径
    # 保存json的路径

    for root, dirs, files in os.walk(file_dir):
        for index, filename in enumerate(files):

            print("begin......" + filename)

            time1 = []
            time2 = []
            time3 = []

            time = (filename.split('_')[-1]).split('.')[0]

            year = time.split('-')[0]
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

            result_dir = result + filename.split('_')[0] + '-' + filename.split('_')[1].split('.')[0]
            json_name_prefix = filename.split('_')[0] + '-' + filename.split('_')[1].split('.')[0]

            stock_id = filename.split('_')[0].split('(')[1].split('.')[0]

            pages = parse_pages(root_file)

            try:
                zcfz_map, lr_map, xjll_map = match_cwfx(pages)
                # list_zyyw = match_zyyw(pages)
                dict3 = write_to_dict_3(result_dir, zcfz_map, lr_map, xjll_map, time1, time2, time3)

                json_name_cwfx = json_name_prefix + '-' + 'cwfx.json'
                # write_to_json(result_dir,json_name_cwfx,dict3)
                print("财务分析done......" + filename)
            except Exception:
                print("财务分析解析错误")

            try:
                list_zyyw = match_zyyw(pages)
                dict2 = write_to_dict_2(result_dir, list_zyyw)
                json_name_zyyw = json_name_prefix + '-' + 'zyyw.json'
                # print(dict2)
                # write_to_json(result_dir, json_name_zyyw, dict2)
                print("主营业务done......" + filename)
            except Exception:
                print("主营业务解析错误")

            try:
                list_cbfx = match_cbfx(pages)
                dict1 = write_to_dict_1(result_dir, list_cbfx)
                json_name_cbfx = json_name_prefix + '-' + 'cbfx.json'
                # write_to_json(result_dir, json_name_cbfx, dict1)
                print("成本分析done......" + filename)
            except Exception:
                print("成本分析解析错误")

            try:
                list_kggs = match_kggs(pages)
                # list_zyyw = match_zyyw(pages)
                dict4 = write_to_dict_4(result_dir, list_kggs)

                json_name_kggs = json_name_prefix + '-' + 'kggs.json'
                # write_to_json(result_dir,json_name_kggs,dict4)
                print("参股控股公司done......" + filename)
            except Exception:
                print("参股控股公司解析错误")
            try:
                gsxx_, lxfs, jbqk, xxpl = match_gsxx(pages)
                # list_zyyw = match_zyyw(pages)
                dict5 = write_to_dict_5(result_dir, gsxx_, lxfs, jbqk, xxpl, stock_id)

                json_name_gsxx = json_name_prefix + '-' + 'gsxx.json'
                # write_to_json(result_dir, json_name_gsxx, dict5)
                print("公司信息done......" + filename)
            except Exception:
                print("公司信息解析错误")

            try:
                yftr = match_yftr(pages)
                # list_zyyw = match_zyyw(pages)
                dict6 = write_to_dict_6(result_dir, yftr)

                json_name_yftr = json_name_prefix + '-' + 'yftr.json'
                # write_to_json(result_dir, json_name_yftr, dict6)
                print("研发投入done......" + filename)
            except Exception:
                print("研发投入解析错误")

            try:
                kjsj, cwzb = match_kjsj_cwzb(pages)
                print(kjsj)
                print(cwzb)
                # list_zyyw = match_zyyw(pages)
                dict7 = write_to_dict_7(result_dir, kjsj, cwzb)
                # print(dict7)
                json_name_kjsj_cwzb = json_name_prefix + '-' + 'kjsj_cwzb.json'
                # write_to_json(result_dir, json_name_kjsj_cwzb, dict7)
                print("主要会计数据和主要财务指标done......" + filename)
            except Exception:
                dict7 = []
                print("主要会计数据和主要财务指标解析错误")


            # write_to_mongoDB(stock_id, time, dict1, dict2, dict3, dict4, dict5, dict6, dict7)
            write_to_mongoDB(stock_id, time, dict6)


if __name__ == '__main__':
    # test()
    # file_dir = 'PDF/'
    # for root, dirs, files in os.walk(file_dir):
    #
    #     for index, filename in enumerate(files):
    #         print(root+filename)
    # print(files)

    main2('PDF', 'result/')
