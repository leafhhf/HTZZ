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
            pages.append({'text':text, 'tables': tables, 'page': index + 1})
        return pages
    except Exception as e:
        print(e)
    return None




# print(pages)

# for index,page in enumerate(pages):
#     print(page['tables'])


# 需要解析的表格中含有的公共字段
table_header_kjsj_cwzb = [['主要会计数据','总资产'],['主要财务指标','扣除非经常性损益后的加权平均净资产收益率（%']]


# 获取解析字段所在的页码
def get_page(table,index,pages,list):

    for i in range(len(table)):
        if table_header_kjsj_cwzb[index][0] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)

    for i in range(len(table)):
        if table_header_kjsj_cwzb[index][1] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)


# 获取解析的信息
def get_information(list_page,tables,list,pages,index_):

    # list表示页码范围
    # tables表示解析的表格
    # map表示要存储的映射
    # pages 表示页码
    # index_ 表示table_header中对应的索引
    if len(list_page) == 1:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                new_p = select_and_delete(p)
                if table_header_kjsj_cwzb[index_][0] in new_p[0][0]:
                    list.extend(new_p)
    elif len(list_page) == 2:
        if pages == list_page[0]:
            for index, p in enumerate(tables):

                new_p = select_and_delete(p)
                if table_header_kjsj_cwzb[index_][0] in new_p[0][0]:
                    list.extend(new_p)
        elif pages == list_page[1]:
            for index, p in enumerate(tables):
                if index == 0:
                    list.extend(select_and_delete(p))



def get_information_2(list_page,tables,list,pages,index_):

    # list表示页码范围
    # tables表示解析的表格
    # map表示要存储的映射
    # pages 表示页码
    # index_ 表示table_header中对应的索引
    if len(list_page) == 1:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                new_p = select_and_delete(p)
                if table_header_kjsj_cwzb[index_][0] in new_p[0][0]:
                    list.extend(p)
    elif len(list_page) == 2:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                new_p = select_and_delete(p)
                if table_header_kjsj_cwzb[index_][0] in new_p[0][0]:
                    # print(p)
                    list.extend(p)
        elif pages == list_page[1]:
            for index, p in enumerate(tables):
                if index == 0:
                    list.extend(p)
    # print(list)



def match_kjsj_cwzb(all_pages):
    list_kjsj = []
    list_cwzb = []



    for index,page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']

        # if pages == 6:
        #     print(tables)


        for index2,table in enumerate(tables):

            # 主要会计数据
            get_page(table, 0, pages, list_kjsj)

            # 主要财务指标
            get_page(table, 1, pages, list_cwzb)

    if (list_kjsj[-1]-list_kjsj[0])>1:
        list_kjsj.remove(list_kjsj[-1])

    # print(list_kjsj)
    # print(list_cwzb)

    kjsj = []
    cwzb = []
    for index, page in enumerate(all_pages):
    #
        tables = page['tables']
        pages = page['page']
    #
    #
        get_information_2(list_kjsj,tables,kjsj,pages,0)
    #
    #     get_information_2(list_lxfs,tables,lxfs,pages,1)
    #
        get_information(list_cwzb,tables,cwzb,pages,1)
    #
    #     get_information(list_xxpl,tables,xxpl,pages,3)
    #
    # print(kjsj)
    # print(cwzb)
    return kjsj,cwzb
    # return gsxx_,lxfs,jbqk,xxpl

# 更换需要解析的pdf的相对路径即可
# pages = parse_pages('PDF/光明乳业/光明乳业(600597.SH)_2017-03-28.pdf')
#
# kjsj,cwzb = match_kjsj_cwzb(pages)

def write_to_dict_7(result_dir,list1,list2):

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    map1 = []
    # print(list1)

    new_list1 = []
    for i in range(len(list1)):
        if list1[i][0] is None or list1[i][0] == '':
            continue
        else:
            new_list1.append(list1[i])
    # print(new_list1)

    new_list1_ = []
    for i in range(len(new_list1)):
        if len(new_list1[i]) < len(new_list1[0]):
            temp_list = []
            for j in range(new_list1[i-1]):
                temp = new_list1[i-1][0]+new_list1[i][0]
                temp_list.append(temp)
                temp_list.append(new_list1[i][1])
                temp_list.append(new_list1[i][2])
                temp_list.append(new_list1[i][3])
                temp_list.append(new_list1[i][4])
            new_list1_.remove(new_list1_[-1])
            new_list1_.append(temp_list)
        else:
            new_list1_.append(new_list1[i])
    # print(new_list1_)

    map1_temp1 = {}
    map1_temp2 = {}
    map1_temp3 = {}
    map1_temp1['date'] = new_list1_[0][1]
    map1_temp2['date'] = new_list1_[0][2]
    map1_temp3['date'] = new_list1_[0][4]
    for i in range(len(new_list1_)):
        if i>=1:
            map1_temp1[split_kongge(new_list1_[i][0])] = new_list1_[i][1]
            map1_temp2[split_kongge(new_list1_[i][0])] = new_list1_[i][2]
            map1_temp3[split_kongge(new_list1_[i][0])] = new_list1_[i][4]
    map1 = [map1_temp1,map1_temp2,map1_temp3]





    map2_temp1 = {}
    map2_temp2 = {}
    map2_temp3 = {}
    map2_temp1['date'] = list2[0][1]
    map2_temp2['date'] = list2[0][2]
    map2_temp3['date'] = list2[0][4]
    for i in range(len(list2)):
        if i>=1:
            map2_temp1[split_kongge(list2[i][0])] = split_kongge(list2[i][1])
            map2_temp2[split_kongge(list2[i][0])] = split_kongge(list2[i][2])
            map2_temp3[split_kongge(list2[i][0])] = split_kongge(list2[i][4])
    map2 = [map2_temp1,map2_temp2,map2_temp3]

    dict = {}
    dict['主要会计数据'] = map1
    dict['主要财务指标'] = map2

    print(dict)

    return dict

# write_to_dict_5(1,kjsj,cwzb)
