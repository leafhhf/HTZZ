import pdfplumber
import os
import xlrd
import re
import json


def select_and_delete(list):
    new_list = []
    for i in range(len(list)):
        new_list_temp = []
        for j in range(len(list[i])):
            if list[i][j] is None or len(list[i][j]) == 0:
                continue
            else:
                new_list_temp.append(list[i][j])
        new_list.append(new_list_temp)

    return new_list


def select_page(list):
    list_select = []
    if len(list) == 1:
        return list

    for i in range(len(list)):
        if i == 0:
            list_select.append(list[i])
        elif (list[i] - list[i - 1]) > 1:
            continue
        else:
            list_select.append(list[i])
    return list_select


def split_kongge(s):
    if isinstance(s, str):

        return "".join((re.sub("\n", " ", s)).split(" "))
    else:
        return s


def split_kongge_list(list):
    new_list = []
    for i in range(len(list)):
        new_list_temp = []
        for j in range(len(list[i])):

            if isinstance(list[i][j], str):
                new_list_temp.append("".join((re.sub("\n", " ", list[i][j])).split(" ")))
            else:
                new_list_temp.append(list[i][j])
        new_list.append(new_list_temp)

    return new_list


# 将PDF内容全部提取
def parse_pages(file_path):
    try:
        pages = []
        pdf = pdfplumber.open(file(path)
        print('parse file:{}   page num:{}'.format(os.path.basename(file(path), len(pdf.pages)))
        for index, page in enumerate(pdf.pages):
            if index == 50:
                break
            tables = page.extract(tables()
            text = page.extract(text()

            if len(tables) < 1:
                continue
            pages.append({'text': text, 'tables': tables, 'page': index + 1})
        return pages
    except Exception as e:
        print(e)
    return None


# print(pages)

# for index,page in enumerate(pages):
#     print(page['tables'])


# 需要解析的表格中含有的公共字段
table(header(yftr = ['本期费用化研发投入', '研发投入资本化的比重']


# 获取解析字段所在的页码
def get(page(table, pages, list):
    for i in range(len(table)):
        if table(header(yftr[0] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)

    for i in range(len(table)):

        if table(header(yftr[1] in table[i]:
            # print(1)
            if pages in list:
                continue
            else:
                list.append(pages)


# 获取解析的信息
def get(information(list(page, tables, map, pages):
    # list表示页码范围
    # tables表示解析的表格
    # map表示要存储的映射
    # pages 表示页码
    # index( 表示table(header中对应的索引
    if len(list(page) == 1:
        if pages == list(page[0]:
            #print('dddddd')
            for index, p in enumerate(tables):
                print('p',p)
                print('table',table(header(yftr[0])
                if table(header(yftr[0] in p[0][0]:
                    for i in range(len(p)):
                        map[split(kongge(p[i][0])] = split(kongge(p[i][1])
    elif len(list(page) == 2:
        if pages == list(page[0]:
            for index, p in enumerate(tables):
                if table(header(yftr[0] in p[0][0]:
                    for i in range(len(p)):
                        map[split(kongge(p[i][0])] = split(kongge(p[i][1])
        elif pages == list(page[1]:
            for index, p in enumerate(tables):
                if index == 0:
                    for i in range(len(p)):
                        map[split(kongge(p[i][0])] = split(kongge(p[i][1])


def match(yftr(all(pages, year):
    list(page(yftr = []

    for index, page in enumerate(all(pages):
        tables = page['tables']
        pages = page['page']

        for index2, table in enumerate(tables):
            get(page(table, pages, list(page(yftr)

    print(list(page(yftr)
    yftr = {}
    yftr['date'] = str(int(year) - 1) + '年'
    print(yftr['date'])
    # list(zcqk = []
    #
    #
    #
    for index, page in enumerate(all(pages):
        tables = page['tables']
        pages = page['page']
        get(information(list(page(yftr, tables, yftr, pages)
    # print(yftr)
    return yftr


# 更换需要解析的pdf的相对路径即可
#pages = parse(pages('PDF/test/同方股份(600100.SH)(2019-04-26.pdf')
#
#yftr = match(yftr(pages, '2019')
#print(yftr)

def write_to_dict_6(result_dir, map):
   # if not os.path.exists(result(dir):
   #     os.makedirs(result(dir)
   # print(map)
    return map

# write(to(dict(6(1,gsxx(,lxfs,jbqk,xxpl,'30000')
