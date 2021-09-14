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


table_header_cbfx = [['分行业情况','分行业','成本构成项目','本期金额','本期占总成本比例(%)','上年同期金额','上年同期占总成本比例(%)','本期金额较上年同期变动比例(%)'],
                     ['分产品情况','分产品','成本构成项目','本期金额','本期占总成本比例(%)','上年同期金额','上年同期占总成本比例(%)','本期金额较上年同期变动比例(%)']]



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


def match_cbfx(all_pages):

    list_page_cbfx = []


    for index,page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        texts = page['text']

        for index2,table in enumerate(tables):

            # 成本分析
            if table[0][0] == table_header_cbfx[0][0]:
                if pages in list_page_cbfx:
                    continue
                else:
                    list_page_cbfx.append(pages)

            for i in range(len(table)):
                if table_header_cbfx[1][0] in table[i]:
                    if pages in list_page_cbfx:
                        continue
                    else:
                        list_page_cbfx.append(pages)


    for index, page in enumerate(all_pages):
        pages = page['page']
        texts = page['text']
        if "成本分析其他情况说明" in texts:
            if pages in list_page_cbfx:
                continue
            else:
                list_page_cbfx.append(pages)

    # print(list_page_cbfx)


    list_cbfx = []

    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        # if len(list_page_cbfx) == 2:
        #     if pages == list_page_cbfx[0]:
        #         print(tables)
        #     if pages == list_page_cbfx[1]:
        #         print(tables)
        # elif len(list_page_cbfx) == 1:
        #     if pages == list_page_cbfx[0]:
        #         print(tables)
    #
        if len(list_page_cbfx) > 0 and list_page_cbfx[0] <= page['page'] and list_page_cbfx[len(list_page_cbfx) - 1] >= \
                page['page']:

            if len(list_page_cbfx) == 1:

                for index2, p in enumerate(page['tables']):
                    new_p = delete_list(p)
                    if new_p[0][0] == table_header_cbfx[0][0]:
                        list_cbfx = p
    #
            elif len(list_page_cbfx) == 2:

                if (list_page_cbfx[1] - list_page_cbfx[0]) == 1:

                    if list_page_cbfx[0] == page['page']:

                        for index2, p in enumerate(page['tables']):
                            new_p = delete_list(p)
                            if new_p[0][0] == table_header_cbfx[0][0]:
                                list_cbfx.extend(p)


                    elif list_page_cbfx[1] == page['page']:

                        for index2, p in enumerate(page['tables']):
                            if index2 == 0:
                                list_cbfx.extend(p)
                elif (list_page_cbfx[1] - list_page_cbfx[0]) > 1:

                    if list_page_cbfx[0] == page['page']:

                        for index2, p in enumerate(page['tables']):
                            new_p = delete_list(p)
                            if new_p[0][0] == table_header_cbfx[0][0]:
                                list_cbfx.extend(p)


                    elif list_page_cbfx[1] == page['page']:

                        for index2, p in enumerate(page['tables']):
                            if index2 == 0:
                                list_cbfx.extend(p)
                    elif page['page']>list_page_cbfx[0] and page['page']<list_page_cbfx[1]:

                            for index2, p in enumerate(page['tables']):

                                list_cbfx.extend(p)


    #
    #         elif len(list_page_zyyw) == 3:
    #
    #             if list_page_zyyw[0] == page['page']:
    #
    #                 for index2, p in enumerate(page['tables']):
    #                     new_p = delete_list(p)
    #                     if new_p[0][0] == table_header_zyyw[0][0]:
    #
    #                         list_zyyw.extend(p)
    #
    #             elif list_page_zyyw[1] == page['page']:
    #
    #                 for index2, p in enumerate(page['tables']):
    #
    #                     list_zyyw.extend(p)
    #
    #
    #             elif list_page_zyyw[2] == page['page']:
    #
    #                 for index2, p in enumerate(page['tables']):
    #
    #                     if index2 == 0:
    #
    #                         list_zyyw.extend(p)
    #
    #
    # # print(list_zyyw)
    # list = []
    # for i in range(len(list_zyyw)):
    #     # print(list_zyyw[i][0])
    #     if list_zyyw[i][0] == '':
    #         continue
    #     else:
    #         list.append(list_zyyw[i])
        #     list_zyyw.remove(list_zyyw[i])
    # print(list_cbfx)
    # print(list)



    return list_cbfx


# 更换需要解析的pdf的相对路径即可
# pages = parse_pages('PDF/光明乳业/光明乳业(600597.SH)_2018-03-27.pdf')
# #
# # pages = parse_pages('PDF_xsb2/佰惠生_835409_[定期报告]佰惠生-2018年度报告_2019-04-22.pdf')
#
# list_cbfx = match_zyyw(pages)


def write_to_dict_1(result_dir,list):

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)


    list1 = []
    list2 = []
    index = []

    for i in range(len(list)):
        if list[i][0] == table_header_cbfx[1][0]:
            index.append(i)
    # print(list)
    #
    # print(index)

    if len(index) == 1:
        for i in range(len(list)):
            if i < index[0]:
                list1.append(list[i])
            else:
                list2.append(list[i])
    else:
        list1 = list
    list1 = list1[2:]
    list2 = list2[2:]
    # print(list1)
    # print(list2)
    dict1 = {}
    list1_index = []
    dict1_ ={}
    dict1_[table_header_cbfx[0][2]] = [table_header_cbfx[0][3],table_header_cbfx[0][4],table_header_cbfx[0][5],table_header_cbfx[0][6],table_header_cbfx[0][7]]
    dict1[table_header_cbfx[0][1]] = dict1_

    for i in range(len(list1)):
        if list1[i][0] is None or list1[i][0] == '':
            continue
        else:
            list1_index.append(i)
    # print(list1_index)
    # print(list1)
    for i in range(len(list1_index)):

        dict1_temp = {}

        if i == len(list1_index)-1:
            for j in range(len(list1)):
                if j >= list1_index[i]:
                    dict1_temp[split_kongge(list1[j][1])] = list1[j][2:7]
        else:
            for j in range(len(list1)):

                if j >= list1_index[i] and j < list1_index[i+1]:

                    dict1_temp[split_kongge(list1[j][1])] = list1[j][2:]

        dict1[split_kongge(list1[list1_index[i]][0])] = dict1_temp

    dict2 = {}

    if list2 != []:

        list2_index = []
        dict2_ = {}
        dict2_[table_header_cbfx[1][2]] = table_header_cbfx[1][3:]
        dict2[table_header_cbfx[1][1]] = dict2_

        # print(list2)

        for i in range(len(list2)):

            if list2[i][0] is None or list2[i][0] == '':
                continue
            else:
                list2_index.append(i)
        # print(list2_index)
        # print(list2)
        for i in range(len(list2_index)):

            dict2_temp = {}

            if i == len(list2_index) - 1:
                for j in range(len(list2)):
                    if j >= list2_index[i]:
                        dict2_temp[split_kongge(list2[j][1])] = list2[j][2:7]
            else:
                for j in range(len(list2)):

                    if j >= list2_index[i] and j < list2_index[i + 1]:
                        dict2_temp[split_kongge(list2[j][1])] = list2[j][2:]

            dict2[split_kongge(list2[list2_index[i]][0])] = dict2_temp

    dict = {}
    dict[table_header_cbfx[0][0]] = dict1
    dict[table_header_cbfx[1][0]] = dict2

    print(dict)


    return dict




# write_to_json_3('PDF',list_cbfx)