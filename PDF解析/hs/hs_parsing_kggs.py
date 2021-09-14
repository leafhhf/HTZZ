import pdfplumber
import os
import xlrd
import re
import json

def split_kongge(s):


    if isinstance(s,str):

        return "".join((re.sub("\n", " ", s)).split(" "))
    else:
        return s


table_header_kggs = ['子公司全称','子公司名称','公司名称','子公司','名称']


def delete_list(list):
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
            pages.append({'text':text, 'tables': tables, 'page': index + 1})
        return pages
    except Exception as e:
        print(e)
    return None



def delete_list_None(list):
    new_list = []
    for i in range(len(list)):
        new_list_temp = []
        for j in range(len(list[i])):
            if list[i][j] is None:
                continue
            else:
                new_list_temp.append(list[i][j])
        new_list.append(new_list_temp)

    return new_list


def delete_dian(value):
    if '.' in value:
        return value.split('.')[1]
    else:
        return value



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

def match_kggs(all_pages):

    list_page_kggs = []

    for index,page in enumerate(all_pages):

        pages = page['page']
        texts = page['text']

        if '主要控股参股公司分析' in texts:
            list_page_kggs.append(pages)
        if '公司控制的结构化主体情况' in texts:
            if pages in list_page_kggs:
                continue
            else:
                list_page_kggs.append(pages)

    flag = -1

    for index,page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']

        # if pages == 31:
        #     print(tables)


        if pages>=list_page_kggs[0] and pages<=list_page_kggs[-1]:

            for index2, table in enumerate(tables):
                new_table = select_and_delete(table)
                if len(new_table) > 0 and (split_kongge(new_table[0][0]) == table_header_kggs[0] or split_kongge(new_table[0][0]) == table_header_kggs[1] or split_kongge(new_table[0][0]) == table_header_kggs[2]
                    or split_kongge(new_table[0][0]) == table_header_kggs[3] or split_kongge(new_table[0][0]) == table_header_kggs[4]):
                    if pages > list_page_kggs[0]:
                        flag = pages


    if flag != -1:
        if flag in list_page_kggs:
            list_page_kggs.remove(list_page_kggs[0])
        else:
            list_page_kggs[0] = flag

    for index,page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']


        if pages == list_page_kggs[-1]:
            if tables == []:
                temp = list_page_kggs[-1] - 1
                list_page_kggs[-1] = temp

    # print(list_page_kggs)
    list_kggs = []
    len_table = 0
    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']

        if pages >= list_page_kggs[0] and pages <= list_page_kggs[-1]:

            if len(list_page_kggs) == 1:
                # print(1)

                for index2, p in enumerate(tables):

                    new_p = select_and_delete(p)
                    # print(new_p)
                    if split_kongge(new_p[0][0]) == table_header_kggs[0] or split_kongge(new_p[0][0]) == table_header_kggs[1] or split_kongge(new_p[0][0]) == table_header_kggs[2] or split_kongge(new_p[0][0]) == table_header_kggs[3] or split_kongge(new_p[0][0]) == table_header_kggs[4] or\
                            ( len(p)>=1 and (split_kongge(new_p[1][0]) == table_header_kggs[0] or split_kongge(new_p[1][0]) == table_header_kggs[1] or split_kongge(new_p[1][0]) == table_header_kggs[2] or split_kongge(new_p[1][0]) == table_header_kggs[3] or split_kongge(new_p[1][0]) == table_header_kggs[4])):
                        list_kggs.extend(p)


            elif len(list_page_kggs) == 2:
                if list_page_kggs[-1] - list_page_kggs[0] == 1:

                    if pages == list_page_kggs[0]:
                        for index2, p in enumerate(tables):

                            new_p = select_and_delete(p)
                            if split_kongge(new_p[0][0]) == table_header_kggs[0] or split_kongge(new_p[0][0]) == table_header_kggs[1] or split_kongge(new_p[0][0]) == table_header_kggs[2] or split_kongge(new_p[0][0]) == table_header_kggs[3] or split_kongge(new_p[0][0]) == \
                                    table_header_kggs[4]:
                                list_kggs.extend(p)
                                len_table = len(p[0])
                                # print(len_table)


                    elif pages == list_page_kggs[-1]:
                        for index2, p in enumerate(tables):
                            # print(p)
                            if index2 == 0:
                                # print(p)
                                # print(len_table)
                                # print(len(p[0]))
                                if len(p[0]) == len_table:
                                    list_kggs.extend(p)
                                else:
                                    continue
                if list_page_kggs[-1] - list_page_kggs[0] > 1:

                    if pages == list_page_kggs[0]:
                        for index2, p in enumerate(tables):

                            new_p = select_and_delete(p)
                            if split_kongge(new_p[0][0]) == table_header_kggs[0] or split_kongge(new_p[0][0]) == table_header_kggs[1] or split_kongge(new_p[0][0]) == table_header_kggs[2] or split_kongge(new_p[0][0]) == table_header_kggs[3] or split_kongge(new_p[0][0]) == \
                                    table_header_kggs[4]:
                                list_kggs.extend(p)
                                len_table = len(p[0])
                                # print(len_table)


                    elif pages == list_page_kggs[-1]:
                        for index2, p in enumerate(tables):
                            # print(p)
                            if index2 == 0:
                                if len(p[0]) == len_table:
                                    list_kggs.extend(p)
                                else:
                                    continue
                    else:
                        for index2, p in enumerate(tables):
                            # print(p)

                            if len(p[0]) == len_table:
                                list_kggs.extend(p)
                            else:
                                break





    # print(list_kggs)

    return list_kggs


# 更换需要解析的pdf的相对路径即可
# pages = parse_pages('test/安徽建工(600502.SH)_2017-03-10.pdf')
# # pages = parse_pages('PDF/号百控股/号百控股(600640.SH)_2019-03-30.pdf')
# #
# list_kggs = match_kggs(pages)


def write_to_dict_4(result_dir,li):

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    # print(li)
    dict = {}

    for i in range(len(li)):
        if i>=1:
            dict_temp = {}
            for j in range(len(li[i])):
                if j>=1:
                    dict_temp[split_kongge(li[0][j])] = split_kongge(li[i][j])
            dict[split_kongge(li[i][0])] = dict_temp
    print(dict)
    # lis = select_and_delete(li)
    # list = []
    # for i in range(len(lis)):
    #     if lis[i] == []:
    #         continue
    #     else:
    #         list.append(lis[i])
    #
    # # print(list)
    # list_1 = []
    # for i in range(len(list)):
    #     list_1.append(split_kongge(list[i][0]))
    #     for j in range(len(list[i])):
    #         if j > 0 :
    #             if "有限公司" in split_kongge(list[i][j]) or "限公司" in split_kongge(list[i][j]) or "公司" in split_kongge(list[i][j]):
    #                 list_1.append(list[i][j])
    # # print(list_1)
    # list_2 = []
    # for i in range(len(list_1)):
    #     if "有限公司" in list_1[i] or "有限责任公司" in list_1[i]:
    #         list_2.append(list_1[i])
    # dict_temp = {}
    # for i in range(len(list_2)):
    #     dict_temp[str(i+1)] = list_2[i]
    # # print(list_2)
    # dict = {}
    # dict['参股控股公司'] = dict_temp
    # print(dict)
    # print(dict)
    return dict




# write_to_dict_4('PDF',list_kggs)