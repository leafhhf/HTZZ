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


# 将PDF内容全部提取
def parse_pages(file_path):
    try:
        pages = []
        pdf = pdfplumber.open(file_path)
        print('parse file:{}   page num:{}'.format(os.path.basename(file_path), len(pdf.pages)))
        for index, page in enumerate(pdf.pages):
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


# print(pages)

# for index,page in enumerate(pages):
#     print(page['tables'])


# 需要解析的表格中含有的公共字段
table_header_yftr = ['本期费用化研发投入', '研发投入资本化的比重（%）']


# 获取解析字段所在的页码
def get_page(table, pages, list):
    for i in range(len(table)):
        if table_header_yftr[0] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)

    for i in range(len(table)):

        if table_header_yftr[1] in table[i]:
            # print(1)
            if pages in list:
                continue
            else:
                list.append(pages)


# 获取解析的信息
def get_information(list_page, tables, map, pages):
    # list表示页码范围
    # tables表示解析的表格
    # map表示要存储的映射
    # pages 表示页码
    # index_ 表示table_header中对应的索引
    if len(list_page) == 1:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                if table_header_yftr[0] in p[0][0]:
                    for i in range(len(p)):
                        map[split_kongge(p[i][0])] = split_kongge(p[i][1])
    elif len(list_page) == 2:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                if table_header_yftr[0] in p[0][0]:
                    for i in range(len(p)):
                        map[split_kongge(p[i][0])] = split_kongge(p[i][1])
        elif pages == list_page[1]:
            for index, p in enumerate(tables):
                if p[-1][0] == table_header_yftr[1]:
                    for i in range(len(p)):
                        map[split_kongge(p[i][0])] = split_kongge(p[i][1])
                # if index == 0:



def match_yftr(all_pages, year):
    list_page_yftr = []

    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        # if pages == 13:
        #     print(tables)

        for index2, table in enumerate(tables):
            get_page(table, pages, list_page_yftr)

    print(list_page_yftr)
    yftr = {}
    yftr['date'] = str(int(year) - 1) + '年'
    # list_zcqk = []
    #
    #
    #
    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        get_information(list_page_yftr, tables, yftr, pages)
    # print(yftr)
    return yftr


# 更换需要解析的pdf的相对路径即可
pages = parse_pages('PDF/test/方正电机(002196.SZ)_2019-04-29.pdf')

yftr = match_yftr(pages, '2017')
print(yftr)

def write_to_dict_6(result_dir, map):
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    print(map)
    return map

#write_to_dict_6(1,gsxx_,lxfs,jbqk,xxpl,'30000')
