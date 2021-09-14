
import pdfplumber
import os
import xlrd
import re
import json

import re




def boolenzh(contents):
    flag = True
    zhmodel = re.compile(u'[\u4e00-\u9fa5]')  # 检查中文
    # zhmodel = re.compile(u'[^\u4e00-\u9fa5]')  #检查非中文
    match = zhmodel.search(contents)
    if match:
        flag =True
    else:
        flag = False
    return flag




def select_page(list):

    list_select = []
    if len(list) == 1:
        return list

    for i in range(len(list)-1):
        if i == 0:
            list_select.append(list[i])
        elif (list[i]-list[i-1]) > 1:
            continue
        else:
            list_select.append(list[i])
    return list_select



def split_kongge(s):


    if isinstance(s,str):

        return "".join((re.sub("\n", " ", s)).split(" "))
    else:
        return s



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



# 需要解析的表格中含有的公共字段
table_header_cwfx = [['项目','流动资产：','负债和所有者权益总计','资产负债表'],
                     ['项目','一、营业总收入','（二）稀释每股收益（元/股）','（二）稀释每股收益','利润表'],
                     ['项目','一、经营活动产生的现金流量：','六、期末现金及现金等价物余额','现金流量表']]



def delete_dian(value):
    if '.' in value:
        return value.split('.')[1]
    else:
        return value

# 获取财务分析中个表的解析信息

def get_information_1(pages,list_page,tables,list):


    if pages >= list_page[0] and pages <= list_page[len(list_page) - 1]:

        if pages == list_page[0]:

            for index, p in enumerate(tables):

                new_p = select_and_delete(p)

                if index == len(tables) - 1:
                    # print(new_p)
                    for k in range(len(new_p)):
                        if k == 0:
                            if len(new_p[k]) == 4:
                                list.append([delete_dian(new_p[k][0]),new_p[k][2],new_p[k][3]])

                            else:
                                list.append(['项目','期末余额','期初余额'])
                        else:
                            # print(new_p)
                            if len(new_p[k]) == 4:
                                list.append([delete_dian(new_p[k][0]),new_p[k][2],new_p[k][3]])
                            elif len(new_p[k]) == 3:
                                if boolenzh(new_p[k][1]):
                                    list.append([delete_dian(new_p[k][0]),new_p[k][2],''])
                                else:
                                    list.append([delete_dian(new_p[k][0]),new_p[k][1],new_p[k][2]])
                            elif len(new_p[k]) == 2:
                                if boolenzh(new_p[k][1]):
                                    list.append([delete_dian(new_p[k][0]),'',''])
                                else:
                                    list.append([delete_dian(new_p[k][0]),new_p[k][1],''])
                            elif len(new_p[k]) == 1:
                                list.append([delete_dian(new_p[k][0]),'',''])
                            else:
                               continue





        elif (list_page[-1] - list_page[0]) > 1 and pages == list_page[len(list_page) - 1]:
            for index, p in enumerate(tables):

                new_p = select_and_delete(p)
                if index == 0:

                    for k in range(len(new_p)):

                        if len(new_p[k]) == 4:
                            list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])
                        elif len(new_p[k]) == 3:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), new_p[k][2], ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], new_p[k][2]])
                        elif len(new_p[k]) == 2:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), '', ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], ''])
                        elif len(new_p[k]) == 1:
                            list.append([delete_dian(new_p[k][0]), '', ''])
                        else:
                            continue



        elif (list_page[-1] - list_page[0]) == 1 and pages == list_page[len(list_page) - 1]:
            for index, p in enumerate(tables):
                new_p = select_and_delete(p)
                if index == 0:
                    for k in range(len(new_p)):

                        if len(new_p[k]) == 4:
                            list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])
                        elif len(new_p[k]) == 3:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), new_p[k][2], ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], new_p[k][2]])
                        elif len(new_p[k]) == 2:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), '', ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], ''])
                        elif len(new_p[k]) == 1:
                            list.append([delete_dian(new_p[k][0]), '', ''])
                        else:
                            continue


        else:
            for index, p in enumerate(tables):
                new_p = select_and_delete(p)
                for k in range(len(new_p)):

                    if len(new_p[k]) == 4:
                        list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])
                    elif len(new_p[k]) == 3:
                        if boolenzh(new_p[k][1]):
                            list.append([delete_dian(new_p[k][0]), new_p[k][2], ''])
                        else:
                            list.append([delete_dian(new_p[k][0]), new_p[k][1], new_p[k][2]])
                    elif len(new_p[k]) == 2:
                        if boolenzh(new_p[k][1]):
                            list.append([delete_dian(new_p[k][0]), '', ''])
                        else:
                            list.append([delete_dian(new_p[k][0]), new_p[k][1], ''])
                    elif len(new_p[k]) == 1:
                        list.append([delete_dian(new_p[k][0]), '', ''])
                    else:
                        continue


def get_information_2(pages,list_page,tables,list):
    if pages >= list_page[0] and pages <= list_page[len(list_page) - 1]:

        if pages == list_page[0]:

            for index, p in enumerate(tables):

                new_p = select_and_delete(p)

                if index == len(tables) - 1:
                    # print(new_p)
                    for k in range(len(new_p)):
                        if k == 0:
                            if len(new_p[k]) == 4:
                                list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])

                            else:
                                list.append(['项目', '本期金额', '上期金额'])
                        else:
                            # print(new_p)
                            if len(new_p[k]) == 4:
                                list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])
                            elif len(new_p[k]) == 3:
                                if boolenzh(new_p[k][1]):
                                    list.append([delete_dian(new_p[k][0]), new_p[k][2], ''])
                                else:
                                    list.append([delete_dian(new_p[k][0]), new_p[k][1], new_p[k][2]])
                            elif len(new_p[k]) == 2:
                                if boolenzh(new_p[k][1]):
                                    list.append([delete_dian(new_p[k][0]), '', ''])
                                else:
                                    list.append([delete_dian(new_p[k][0]), new_p[k][1], ''])
                            elif len(new_p[k]) == 1:
                                list.append([delete_dian(new_p[k][0]), '', ''])
                            else:
                                continue





        elif (list_page[-1] - list_page[0]) > 1 and pages == list_page[len(list_page) - 1]:
            for index, p in enumerate(tables):

                new_p = select_and_delete(p)
                if index == 0:

                    for k in range(len(new_p)):

                        if len(new_p[k]) == 4:
                            list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])
                        elif len(new_p[k]) == 3:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), new_p[k][2], ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], new_p[k][2]])
                        elif len(new_p[k]) == 2:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), '', ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], ''])
                        elif len(new_p[k]) == 1:
                            list.append([delete_dian(new_p[k][0]), '', ''])
                        else:
                            continue



        elif (list_page[-1] - list_page[0]) == 1 and pages == list_page[len(list_page) - 1]:
            for index, p in enumerate(tables):
                new_p = select_and_delete(p)
                if index == 0:
                    for k in range(len(new_p)):

                        if len(new_p[k]) == 4:
                            list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])
                        elif len(new_p[k]) == 3:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), new_p[k][2], ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], new_p[k][2]])
                        elif len(new_p[k]) == 2:
                            if boolenzh(new_p[k][1]):
                                list.append([delete_dian(new_p[k][0]), '', ''])
                            else:
                                list.append([delete_dian(new_p[k][0]), new_p[k][1], ''])
                        elif len(new_p[k]) == 1:
                            list.append([delete_dian(new_p[k][0]), '', ''])
                        else:
                            continue


        else:
            for index, p in enumerate(tables):
                new_p = select_and_delete(p)
                for k in range(len(new_p)):

                    if len(new_p[k]) == 4:
                        list.append([delete_dian(new_p[k][0]), new_p[k][2], new_p[k][3]])
                    elif len(new_p[k]) == 3:
                        if boolenzh(new_p[k][1]):
                            list.append([delete_dian(new_p[k][0]), new_p[k][2], ''])
                        else:
                            list.append([delete_dian(new_p[k][0]), new_p[k][1], new_p[k][2]])
                    elif len(new_p[k]) == 2:
                        if boolenzh(new_p[k][1]):
                            list.append([delete_dian(new_p[k][0]), '', ''])
                        else:
                            list.append([delete_dian(new_p[k][0]), new_p[k][1], ''])
                    elif len(new_p[k]) == 1:
                        list.append([delete_dian(new_p[k][0]), '', ''])
                    else:
                        continue



def match_cwfx(all_pages):
    list_page_zcfz = []
    list_page_lr = []
    list_page_xjll = []



    for index,page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        text = page['text']



        for index2,table in enumerate(tables):
            # 资产负债
            for i in range(len(table)):
                if len(list_page_zcfz) == 2:
                    break
                if table_header_cwfx[0][1] in table[i]:
                    list_page_zcfz.append(pages)
                if table_header_cwfx[0][2] in table[i]:
                    list_page_zcfz.append(pages)

            # 利润表
            for i in range(len(table)):
                if len(list_page_lr) == 2:
                    break
                if table_header_cwfx[1][1] in table[i]:
                    list_page_lr.append(pages)
                if table_header_cwfx[1][2] in table[i] or table_header_cwfx[1][3] in table[i]:
                    list_page_lr.append(pages)


            # 现金流量表
            for i in range(len(table)):
                if len(list_page_xjll) == 2:
                    break
                if table_header_cwfx[2][1] in table[i]:
                    list_page_xjll.append(pages)
                if table_header_cwfx[2][2] in table[i] :
                    list_page_xjll.append(pages)

    zcfz = []
    lr = []
    xjll = []


    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']

        # 资产负债表
        get_information_1(pages, list_page_zcfz, tables, zcfz)
        # 利润表
        get_information_2(pages, list_page_lr, tables,lr)
        # 现金流量表
        get_information_2(pages, list_page_xjll, tables, xjll)
    # print("资产负债表：" + str(zcfz_map))
    # print("利润表：" +str(lr_map))
    # print("现金流量：" + str(xjll_map))
    # print(zcfz)
    # print(lr)
    # print(xjll)

    return zcfz,lr,xjll



def delete_list(list):
    new_list = []

    for i in range(len(list)):
        if list[i] is None or list[i] == '':
            continue
        else:
            new_list.append(list[i])


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

#
# pages = parse_pages('PDF/安徽建工/安徽建工(600502.SH)_2019-03-21.pdf')
# list_zcfz,list_lr,list_xjll = match_cwfx(pages)


# for key, value in map2.items():
#     print(key + value)
def write_to_dict_3(result_dir,list1,list2,list3,time1,time2,time3):

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)



    dict1 = []
    dict1_temp1 = {}
    dict1_temp2 = {}
    if list1[0][0] == '项目':


        dict1_temp1['date'] = time1[0]
        dict1_temp2['date'] = time1[1]
        for i in range(len(list1)):
            if i >= 1:
                dict1_temp1[split_kongge(delete_dian(list1[i][0]))] = list1[i][1]
                dict1_temp2[split_kongge(delete_dian(list1[i][0]))] = list1[i][2]
    else:
        dict1_temp1['date'] = time1[0]
        dict1_temp2['date'] = time1[1]
        for i in range(len(list1)):
            dict1_temp1[split_kongge(delete_dian(list1[i][0]))] = list1[i][1]
            dict1_temp2[split_kongge(delete_dian(list1[i][0]))] = list1[i][2]

    dict1.append(dict1_temp1)
    dict1.append(dict1_temp2)
    # print(dict1)

    dict2 = []
    dict2_temp1 = {}
    dict2_temp2 = {}
    if list2[0][0] == '项目':
        dict2_temp1['date'] = time2[0]
        dict2_temp2['date'] = time2[1]
        for i in range(len(list2)):
            if i >= 1:
                dict2_temp1[split_kongge(delete_dian(list2[i][0]))] = list2[i][1]
                dict2_temp2[split_kongge(delete_dian(list2[i][0]))] = list2[i][2]
    else:
        dict2_temp1['date'] = time2[0]
        dict2_temp2['date'] = time2[1]
        for i in range(len(list2)):
            dict2_temp1[split_kongge(delete_dian(list2[i][0]))] = list2[i][1]
            dict2_temp2[split_kongge(delete_dian(list2[i][0]))] = list2[i][2]
    dict2 = [dict2_temp1,dict2_temp2]


    # print(dict2)

    dict3 = []
    dict3_temp1 = {}
    dict3_temp2 = {}
    if list3[0][0] == '项目':
        dict3_temp1['date'] = time3[0]
        dict3_temp2['date'] = time3[1]

        for i in range(len(list3)):
            if i >= 1:
                dict3_temp1[split_kongge(delete_dian(list3[i][0]))] = list3[i][1]
                dict3_temp2[split_kongge(delete_dian(list3[i][0]))] = list3[i][2]
    else:
        dict3_temp1['date'] = time3[0]
        dict3_temp2['date'] = time3[1]
        for i in range(len(list3)):
            dict3_temp1[split_kongge(delete_dian(list3[i][0]))] = list3[i][1]
            dict3_temp2[split_kongge(delete_dian(list3[i][0]))] = list3[i][2]
    dict3.append(dict3_temp1)
    dict3.append(dict3_temp2)
    # print(dict3)

    dict = {}
    dict['资产负债表'] = dict1
    dict['利润表'] = dict2
    dict['现金流量表'] = dict3

    print(dict)


    return dict


# write_to_dict_3('1',list_zcfz,list_lr,list_xjll,['2019-04-22','2019-04-22'],['2019-04-22','2019-04-22'],['2019-04-22','2019-04-22'])
