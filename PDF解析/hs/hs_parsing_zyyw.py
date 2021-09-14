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


table_header_zyyw = [['主营业务分行业情况','分行业','营业收入','营业成本','毛利率（%）'],['主营业务分产品情况','分产品','营业收入','营业成本','毛利率（%）'],
                     ['主营业务分地区情况','分地区','营业收入','营业成本','毛利率（%）']]


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


def match_zyyw(all_pages):

    list_page_zyyw = []


    for index,page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        texts = page['text']

        for index2,table in enumerate(tables):

            # 主营业务
            if table[0][0] == table_header_zyyw[0][0]:
                if pages in list_page_zyyw:
                    continue
                else:
                    list_page_zyyw.append(pages)

            for i in range(len(table)):
                if table_header_zyyw[1][0] in table[i]:
                    if pages in list_page_zyyw:
                        continue
                    else:
                        list_page_zyyw.append(pages)
                if table_header_zyyw[2][0] in table[i]:
                    if pages in list_page_zyyw:
                        continue
                    else:
                        list_page_zyyw.append(pages)

    for index, page in enumerate(all_pages):
        pages = page['page']
        texts = page['text']
        if "主营业务分行业、分产品、分地区情况的说明" in texts:
            if pages in list_page_zyyw:
                continue
            else:
                list_page_zyyw.append(pages)
        # list_page_qy.remove(list_page_qy[0])
    # print(list_page_zyyw)
    # print(list_page_cp)
    # print(list_page_qy)

    list_zyyw = []
    zyyw_map = {}

    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        # if len(list_page_zyyw) == 2:
        #     if pages == list_page_zyyw[0]:
        #         print(tables)
        #     if pages == list_page_zyyw[1]:
        #         # print(tables)
        # elif len(list_page_zyyw) == 1:
        #     if pages == list_page_zyyw[0]:
        #         print(tables)

        if len(list_page_zyyw) > 0 and list_page_zyyw[0] <= page['page'] and list_page_zyyw[len(list_page_zyyw) - 1] >= \
                page['page']:
            if len(list_page_zyyw) == 1:

                for index2, p in enumerate(page['tables']):
                    new_p = delete_list(p)
                    if new_p[0][0] == table_header_zyyw[0][0]:
                        list_zyyw = p

            elif len(list_page_zyyw) == 2:

                if list_page_zyyw[0] == page['page']:

                    for index2, p in enumerate(page['tables']):
                        new_p = delete_list(p)
                        if new_p[0][0] == table_header_zyyw[0][0]:
                            list_zyyw.extend(p)


                elif list_page_zyyw[1] == page['page']:

                    for index2, p in enumerate(page['tables']):
                        if index2 == 0:
                            list_zyyw.extend(p)

            elif len(list_page_zyyw) == 3:

                if list_page_zyyw[0] == page['page']:

                    for index2, p in enumerate(page['tables']):
                        new_p = delete_list(p)
                        if new_p[0][0] == table_header_zyyw[0][0]:

                            list_zyyw.extend(p)

                elif list_page_zyyw[1] == page['page']:

                    for index2, p in enumerate(page['tables']):

                        list_zyyw.extend(p)


                elif list_page_zyyw[2] == page['page']:

                    for index2, p in enumerate(page['tables']):

                        if index2 == 0:

                            list_zyyw.extend(p)


    # print(list_zyyw)
    list = []
    for i in range(len(list_zyyw)):
        # print(list_zyyw[i][0])
        if list_zyyw[i][0] == '':
            continue
        else:
            list.append(list_zyyw[i])
        #     list_zyyw.remove(list_zyyw[i])
    # print(list_zyyw)
    # print(list)



    return list


# 更换需要解析的pdf的相对路径即可
# pages = parse_pages('PDF/白云机场/白云机场(600004.SH)_2019-04-30.pdf')
# #
# # # pages = parse_pages('PDF_xsb2/佰惠生_835409_[定期报告]佰惠生-2018年度报告_2019-04-22.pdf')
# #
# list_zyyw = match_zyyw(pages)



def write_to_dict_2(result_dir,list):

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)


    list1 = []
    list2 = []
    list3 = []
    index = []

    for i in range(len(list)):
        if list[i][0] == table_header_zyyw[1][0] or list[i][0] == table_header_zyyw[2][0]:
            index.append(i)
    # print(list)
    #
    # print(index)

    if len(index) == 2:
        for i in range(len(list)):
            if i < index[0]:
                list1.append(list[i])
            elif index[0] <= i < index[1]:
                list2.append(list[i])
            else:
                list3.append(list[i])
    elif len(index) == 0:
        list1 = list
    # print(list1)
    # print(list2)
    # print(list3)

    dict1 = {}
    dict2 = {}
    dict3 = {}
    dict = {}
    for i in range(len(list1)):
        dict1_temp = {}
        if i<2:
            continue
        else:
            for j in range(len(list1[i])):
                if j < 4:
                    dict1_temp[table_header_zyyw[0][j+1]] = split_kongge(list1[i][j])
        dict1[split_kongge(list1[i][0])] = dict1_temp
    # print(dict1)
    if list2 == []:
        dict2[''] = ''
    else:
        for i in range(len(list2)):
            dict2_temp = {}
            if i < 2:
                continue
            else:
                for j in range(len(list2[i])):
                    if j < 4:
                        dict2_temp[table_header_zyyw[1][j + 1]] = split_kongge(list2[i][j])
            dict2[split_kongge(list2[i][0])] = dict2_temp


    if list3 == []:
        dict3[''] = ''
    else:
        for i in range(len(list3)):
            dict3_temp = {}
            if i < 2:
                continue
            else:
                for j in range(len(list3[i])):
                    if j < 4:
                        dict3_temp[table_header_zyyw[1][j + 1]] = split_kongge(list3[i][j])
            dict3[split_kongge(list3[i][0])] = dict3_temp
    # print(dict2)
    # print(dict3)
    dict[table_header_zyyw[0][0]] = dict1
    dict[table_header_zyyw[1][0]] = dict2
    dict[table_header_zyyw[2][0]] = dict3

    print(dict)

    return dict






    # dict1 = {}
    # index = 1
    # for i in range(len(list1)):
    #     dict_temp = {}
    #     for j in range(len(list1[i])):
    #             dict_temp[table_header_zyyw[0][j]] = split_kongge(list1[i][j])
    #
    #     dict1[index] = dict_temp
    #     index = index+1
    # # print(dict1)
    #
    # dict2 = {}
    # index2 = 1
    # for i in range(len(list2)):
    #     dict_temp_2 = {}
    #     for j in range(len(list2[i])):
    #
    #             dict_temp_2[table_header_zyyw[1][j]] = split_kongge(list2[i][j])
    #
    #     dict2[index2] = dict_temp_2
    #     index2 = index2 + 1
    # # print(dict2)
    #
    # dict = {}
    # dict[table_header_zyyw[0][6]] = dict1
    # dict[table_header_zyyw[1][6]] = dict2
    # # print(dict)
    #
    #
    #
    #
    #
    # with open(result_dir+'/'+'zyyw.json', 'w', encoding='utf-8') as f:
    #     json.dump(dict, f, indent=4,ensure_ascii=False)

# write_to_json(1,list_cp,list_qy)