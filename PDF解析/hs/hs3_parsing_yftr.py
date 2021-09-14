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
        pdf = pdfplumber.open(file_path)
        print('parse file:{}   page num:{}'.format(os.path.basename(file_path), len(pdf.pages)))
        for index, page in enumerate(pdf.pages):
            if index == 50:
                print('index',index)
                break
            
            tables = page.extract_tables()
            text = page.extract_text()
            if index == 15:
                if text == '研发投入情况表':
                    print('text',text)
                

            if len(tables) < 1 and len(tables)>15:
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
table_header_yftr = ['研发投入', '研发投入情况表']


# 获取解析字段所在的页码
def get_page(text, pages, list):
    #table = split_kongge_list(table)

    for i in range(len(text)):
        if table_header_yftr[1] in text[i] or table_header_yftr[0] in text[i]:
            if pages in list:
                continue
            else:
                list.append(pages)

  

# 获取解析的信息
def get_information(list_page, tables, list, pages):
    # list表示页码范围
    # tables表示解析的表格
    # map表示要存储的映射
    # pages 表示页码
    # index_ 表示table_header中对应的索引
    if len(list_page) == 1:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                # print(p)
                if len(p) == 1:
                    continue
                if (p[0][0] is None) or (p[1][0] is None):
                    continue
                if table_header_yftr[1] in p[0][0] or table_header_yftr[1] in p[1][0] or table_header_yftr[0] in p[0][0] or table_header_yftr[0] in p[1][0]:
                    list.extend(p)
    elif len(list_page) == 2:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                print(p)
                if len(p) == 1:
                    continue
                if (p[0][0] is None) or (p[1][0] is None):
                    continue
                if table_header_yftr[1] in p[0][0] or table_header_yftr[1] in p[1][0] or table_header_yftr[0] in p[0][0] or table_header_yftr[0] in p[1][0]:
                    list.extend(p)
        elif pages == list_page[1]:
            for index, p in enumerate(tables):
                if index == 0:
                    list.extend(p)


def match_yftr2(all_pages):
    list_page_yftr = []

    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        texts = page['text']
        # if pages == 18:
        #     print(tables)

        for index2, text in texts:
            print(text)
            aa=get_page(text, pages, list_page_yftr)
            #if aa != 'None':
                #print('aa',aa)
    #
    print(list_page_yftr)
    # yftr = {}
    #
    # list_zcqk = []
    #
    #
    yftr = []
    #
    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        # if pages == 30:
        #     print(tables)
        #get_information(list_page_yftr, tables, yftr, pages)
    # print(yftr)
    return yftr


# 更换需要解析的pdf的相对路径即可
pages = parse_pages('PDF/test/ST安通(600179.SH)_2018-03-13.pdf')
#
yftr = match_yftr2(pages)
#
#print(yftr)

#
def hebing(list):
    new_list = []
    temp = 0
    for i in range(len(list)):

        if i == temp:
            if list[i][0] is None:
                list_temp = []
                list_temp.append(list[i - 1][0] + list[i + 1][0])
                list_temp.append(list[i][1])
                list_temp.append(list[i][2])
                list_temp.append(list[i][3])
                new_list.remove(new_list[-1])
                new_list.append(list_temp)
                temp = i + 3
            else:
                new_list.append(list[i])
                temp = i + 1
        else:
            continue
    return new_list


def write_to_dict_6_2(result_dir, list, time):
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    list = hebing(list)
    # print(list)

    dict = []
    if list[0][0] == '':
        dict_temp = {}
        dict_temp['date'] = str(int(time))
        for i in range(len(list)):
            if i == 0:
                dict_temp['date'] = split_kongge(list[i][1])
            if i >= 1:
                dict_temp[split_kongge(list[i][0])] = split_kongge(list[i][1])

    elif list[0][0] != '':
        dict_temp = {}
        dict_temp['date'] = str(int(time))
        for i in range(len(list)):
            dict_temp[split_kongge(list[i][0])] = split_kongge(list[i][1])


    print(dict_temp)
    return dict_temp


# write_to_dict_6_2(1, yftr, '2019')
