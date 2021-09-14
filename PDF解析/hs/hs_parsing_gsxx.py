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
table_header_gsxx = [['公司的中文','公司的法定代表人'],['董事会秘书','电子信箱'],['公司注册地址','电子信箱'],
               ['公司选定的信息披露媒体名称','公司年度报告备置地点'],['公司股票简况','股票种类']]


# 获取解析字段所在的页码
def get_page(table,index,pages,list):
    # table = select_and_delete(table)

    # if table[0][0] == table_header_gsxx[index][0] or (len(table)>1 and (table[1][0] == table_header_gsxx[index][0] or (len(table_header_gsxx[index])>2 and table[1][0] == table_header_gsxx[index][2]) or (len(table_header_gsxx[index])>3 and table[1][0] == table_header_gsxx[index][3]))):
    #     list.append(pages)


    for i in range(len(table)):
        if table_header_gsxx[index][0] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)

    for i in range(len(table)):
        if table_header_gsxx[index][1] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)


# 获取解析的信息
def get_information(list_page,tables,map,pages,index_):

    # list表示页码范围
    # tables表示解析的表格
    # map表示要存储的映射
    # pages 表示页码
    # index_ 表示table_header中对应的索引
    if len(list_page) == 1:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                if table_header_gsxx[index_][0] in p[0][0]:
                    for i in range(len(p)):
                        map[split_kongge(p[i][0])] = split_kongge(p[i][1])
    elif len(list_page) == 2:
        if pages == list_page[0]:
            for index, p in enumerate(tables):
                if table_header_gsxx[index_][0] in p[0][0]:
                    for i in range(len(p)):
                        map[split_kongge(p[i][0])] = split_kongge(p[i][1])
        elif pages == list_page[1]:
            for index, p in enumerate(tables):
                if index == 0:
                    for i in range(len(p)):
                        map[split_kongge(p[i][0])] = split_kongge(p[i][1])



# 获取解析的信息
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
                if table_header_gsxx[index_][0] in new_p[0][0]:
                    list.extend(new_p)
    elif len(list_page) == 2:
        if pages == list_page[0]:
            for index, p in enumerate(tables):

                new_p = select_and_delete(p)
                if table_header_gsxx[index_][0] in new_p[0][0]:
                    list.extend(new_p)
        elif pages == list_page[1]:
            for index, p in enumerate(tables):
                if len(p[0])==3:
                    for i in range(len(p)):
                        list.extend(select_and_delete(p))



def match_gsxx(all_pages):
    list_gsxx_ = []
    list_lxfs = []
    list_jbqk = []
    list_xxpl = []


    for index,page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        #
        # if pages == 6:
        #     print(tables)

        for index2,table in enumerate(tables):

            # 公司信息
            get_page(table, 0, pages, list_gsxx_)

            # 联系方式
            get_page(table, 1, pages, list_lxfs)

            # 基本情况
            # get_page(table,2,pages,list_jbqk)
            for i in range(len(table)):
                if table_header_gsxx[2][0] in table[i]:
                    if pages in list_jbqk:
                        continue
                    else:
                        list_jbqk.append(pages)
            # print(list_jbqk)
            for i in range(len(table)):
                if table_header_gsxx[2][1] in table[i]:
                    if pages in list_jbqk:
                        continue
                    else:
                        list_jbqk.append(pages)
            # print()

            # 信息披露以及备置地点
            get_page(table,3,pages,list_xxpl)
    # print(list_jbqk)

    # print(list_page_zcqk)
    list_gsxx_ = select_page(list_gsxx_)
    list_lxfs = select_page(list_lxfs)

    list_jbqk = select_page(list_jbqk)
    if len(list_lxfs) == 2:
        if list_lxfs[-1]>list_jbqk[0]:
            list_lxfs.remove(list_lxfs[-1])
    list_xxpl = select_page(list_xxpl)
    # print(list_gsxx_)
    # print(list_lxfs)
    #
    # print(list_jbqk)
    # print(list_xxpl)


    # new_list_page_zcqk = []
    # if len(list_page_zcqk) >1:
    #     for i in range(len(list_page_zcqk)):
    #         if i == 0:
    #             new_list_page_zcqk.append(list_page_zcqk[0])
    #         else:
    #             if (list_page_zcqk[i]-list_page_zcqk[0])>1:
    #                 continue
    #             elif (list_page_zcqk[i]-list_page_zcqk[0])==1:
    #                 new_list_page_zcqk.append(list_page_zcqk[i])
    # elif len(list_page_zcqk) == 1:
    #     new_list_page_zcqk.append(list_page_zcqk[0])

    # print(new_list_page_zcqk)
    # print("基本信息所在范围："+str(list_jibenxx))
    # print("联系方式所在范围：" + str(list_lxfs))
    # print("企业信息所在范围：" + str(list_qyxx))


    gsxx_ = {}
    lxfs = []
    jbqk = {}
    xxpl = {}
    # list_zcqk = []
    #
    #
    #
    for index, page in enumerate(all_pages):

        tables = page['tables']
        pages = page['page']


        get_information(list_gsxx_,tables,gsxx_,pages,0)

        get_information_2(list_lxfs,tables,lxfs,pages,1)

        get_information(list_jbqk,tables,jbqk,pages,2)

        get_information(list_xxpl,tables,xxpl,pages,3)

    #
    # print(gsxx_)
    # print(lxfs)
    # print(jbqk)
    # print(xxpl)
    #
    #
    #     #注册情况
    #     get_information_2(new_list_page_zcqk,tables,list_zcqk,pages,3)
    #
    #
    # if list_zcqk[0][0] == '统一社会信用代码':
    #     list_zcqk = list_zcqk[0:3]
    # else:
    #     list_zcqk = list_zcqk[1:4]
    #
    # zcqk = {}
    # for i in range(len(list_zcqk)):
    #     zcqk[split_kongge(list_zcqk[i][0])] = split_kongge(list_zcqk[i][1])
    #
    # # print('基本信息:'+str(jibenxx))
    # # print('联系方式:'+str(lxfs))
    # # print("企业信息：" + str(qyxx))
    # # print(zcqk)
    # return 1
    return gsxx_,lxfs,jbqk,xxpl

# 更换需要解析的pdf的相对路径即可
# pages = parse_pages('PDF/纵横通信/纵横通信(603602.SH)_2019-04-04.pdf')
#
# gsxx_,lxfs,jbqk,xxpl = match_gsxx(pages)

def write_to_dict_5(result_dir,map1,list2,map3,map4,stock_id):

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    map2 = {}
    map2_temp1 = {}
    map2_temp2 = {}
    for i in range(len(list2)):
        if i>=1:
            map2_temp1[split_kongge(list2[i][0])] = split_kongge(list2[i][1])
            map2_temp2[split_kongge(list2[i][0])] = split_kongge(list2[i][2])
    map2[split_kongge(list2[0][0])] = map2_temp1
    map2[split_kongge(list2[0][1])] = map2_temp2

    dict = {}
    dict['公司信息'] = map1
    dict['联系人和联系方式'] = map2
    dict['基本情况介绍'] = map3
    dict['信息披露及备置地点'] = map4
    dict['股票代码'] = stock_id
    print(dict)

    return dict

# write_to_dict_5(1,gsxx_,lxfs,jbqk,xxpl,'30000')
