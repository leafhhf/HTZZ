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

# 删除list中带有‘’或者None的字段
def delete_list(list):
    new_list = []
    for i in range(len(list)):
        new_list_temp = []
        for j in range(len(list[i])):
            if list[i][j] is None:
                new_list_temp.append(list[i][j])
                continue
            if len(list[i][j]) == 0:
                continue
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
table_header_yftr = ['研发投入金额（元）', '研发人员数量（人）', '资本化研发支出占当期净利', '资本化研发支出占研发投入', '资本化研发投入占研发投入']


# 获取解析字段所在的页码
def get_page(table, pages, list):
    table = split_kongge_list(table)

    for i in range(len(table)):
        if table_header_yftr[1] in table[i] or table_header_yftr[0] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)

    for i in range(len(table)):

        if table_header_yftr[2] in table[i] or table_header_yftr[3] in table[i] or table_header_yftr[4] in table[i]:
            # print(1)
            # print(pages)
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
                p = delete_list(p)
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
                # print(p)
                p = delete_list(p)
                # print(p)
                if len(p) == 1:
                    continue
                if (p[0][0] is None) or (p[1][0] is None):
                    continue
                if table_header_yftr[1] in p[0][0] or table_header_yftr[1] in p[1][0] or table_header_yftr[0] in p[0][0] or table_header_yftr[0] in p[1][0]:
                    list.extend(p)
        elif pages == list_page[1]:
            for index, p in enumerate(tables):
                p = delete_list(p)
                # print(p)
                if index == 0:
                    list.extend(p)


def match_yftr2(all_pages):
    list_page_yftr = []

    for index, page in enumerate(all_pages):
        tables = page['tables']
        pages = page['page']
        # if pages == 28:
        #     print(tables)

        for index2, table in enumerate(tables):
            get_page(table, pages, list_page_yftr)
    #
    # print(list_page_yftr)
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
        # if pages == 28:
        #     print(tables)
        get_information(list_page_yftr, tables, yftr, pages)
    # print(yftr)
    return yftr


# 更换需要解析的pdf的相对路径即可
pages = parse_pages('PDF/爱迪尔(002740.SZ)_2019-06-18.pdf')
#
yftr = match_yftr2(pages)
#
print(yftr)

#
def hebing(list):
    new_list = []
    
    temp = 0
    list=select_and_delete(list)
    list=[ x for x in list if x!=[]]
   # print('lllllllllllll',list)
    for i in range(len(list)):
       # print('list[i]',list[i],i,len(list[i]))
        if '变动比例' in list[0]:
            list_temp = []
            if i==0:
                list_temp.append(list[i][0])
                list_temp.append(list[i][1])
            elif len(list[i])==4:
                if '）' in list[i]:
                    list_temp.append(list[i][0]+'）')
                    list_temp.append(list[i][2])
                    list_temp.append(list[i][3])
                else:
                   # print('aaaaaaa')
                    list_temp.append(list[i][0])
                    list_temp.append(list[i][1])
                    list_temp.append(list[i][2])
            elif '资本化研发投入占研发投入' in list[i] :
                #print('bbbbbbbbbbb')
                #and len(list[i])==1 and len(list[i+1])==2 and len(list[i+2])==1
                list_temp.append(list[i][0]+list[i+2][0])
                list_temp.append(list[i+1][0])
                list_temp.append(list[i+1][1])
           
        else:
            list_temp = []
            if i==0:
                list_temp.append(list[i][0])
                list_temp.append(list[i][1])
                list_temp.append(list[i][2])
            elif len(list[i])==4:
                if '）' in list[i]:
                    list_temp.append(list[i][0]+'）')
                    list_temp.append(list[i][2])
                    list_temp.append(list[i][3])
                    list_temp.append(list[i][4])
                else:
                    
                    list_temp.append(list[i][0])
                    list_temp.append(list[i][1])
                    list_temp.append(list[i][2])
                    list_temp.append(list[i][3])
            elif '资本化研发投入占研发投入' in list[i] :
               
                #and len(list[i])==1 and len(list[i+1])==2 and len(list[i+2])==1
                list_temp.append(list[i][0]+list[i+2][0])
                list_temp.append(list[i+1][0])
                list_temp.append(list[i+1][1])
       # print('list_temp',list_temp)
        new_list.append(list_temp)
   # print('new_list',new_list)
    return new_list

def write_to_dict_6_2(result_dir, list, time):
    #if not os.path.exists(result_dir):
   #     os.makedirs(result_dir)
    # print(list)
    list = hebing(list)
    list=[ x for x in list if x!=[]]
    #print('after',list)
    dict = []
    if list[0][0] == '':
        if '%' in list[1][-1]:
            dict_temp1 = {}
            dict_temp2 = {}
            dict_temp1['date'] = str(int(time) - 1)
            dict_temp2['date'] = str(int(time) - 2)
            for i in range(len(list)):
                if i == 0:
                    dict_temp1['date'] = split_kongge(list[i][1])
                    dict_temp2['date'] = split_kongge(list[i][2])
                if i >= 1:
                    dict_temp1[split_kongge(list[i][0])] = split_kongge(list[i][1])
                    dict_temp2[split_kongge(list[i][0])] = split_kongge(list[i][2])
            dict = [dict_temp1, dict_temp2]
        else:
            dict_temp1 = {}
            dict_temp2 = {}
            dict_temp3 = {}
            dict_temp1['date'] = str(int(time) - 1)
            dict_temp2['date'] = str(int(time) - 2)
            dict_temp3['date'] = str(int(time) - 3)
            for i in range(len(list)):
                if i == 0:
                    dict_temp1['date'] = split_kongge(list[i][1])
                    dict_temp2['date'] = split_kongge(list[i][2])
                    dict_temp3['date'] = split_kongge(list[i][3])
                if i >= 1:
                    dict_temp1[split_kongge(list[i][0])] = split_kongge(list[i][1])
                    dict_temp2[split_kongge(list[i][0])] = split_kongge(list[i][2])
                    dict_temp3[split_kongge(list[i][0])] = split_kongge(list[i][3])
            dict = [dict_temp1, dict_temp2, dict_temp3]

       

    elif list[0][0] != '':
        if len(list[0]) == 2:
            dict_temp1 = {}
            dict_temp2 = {}
            dict_temp1['date'] = str(int(time) - 1)
            dict_temp2['date'] = str(int(time) - 2)
            print('list[0]',list[0])
            for i in range(len(list)):
                    if i == 0:
                        dict_temp1['date'] = split_kongge(list[i][0])
                        dict_temp2['date'] = split_kongge(list[i][1])
                    if i >= 1:
                        if split_kongge(list[i][0]) == '研发投入资本化的金额（元':
                            dict_temp1['研发投入资本化的金额（元）'] = split_kongge(list[i][2])
                            dict_temp2['研发投入资本化的金额（元）'] = split_kongge(list[i][3])
                            continue
                        dict_temp1[split_kongge(list[i][0])] = split_kongge(list[i][1])
                        dict_temp2[split_kongge(list[i][0])] = split_kongge(list[i][2])
            dict = [dict_temp1, dict_temp2]     
                
                
            '''
            else:
                for i in range(len(list)):
                    if list[i][0] == '项目':
                        continue
                    dict_temp1[split_kongge(list[i][0])] = split_kongge(list[i][1])
                    dict_temp2[split_kongge(list[i][0])] = split_kongge(list[i][2])
                dict = [dict_temp1, dict_temp2]
                '''
        else:
            dict_temp1 = {}
            dict_temp2 = {}
            dict_temp3 = {}
            dict_temp1['date'] = str(int(time) - 1)
            dict_temp2['date'] = str(int(time) - 2)
            dict_temp3['date'] = str(int(time) - 3)
            if len(list[0]) == 3:
                for i in range(len(list)):
                    if i == 0:
                        dict_temp1['date'] = split_kongge(list[i][0])
                        dict_temp2['date'] = split_kongge(list[i][1])
                        dict_temp3['date'] = split_kongge(list[i][2])
                    if i >= 1:
                        if split_kongge(list[i][0]) == '研发投入资本化的金额（元':
                            dict_temp1['研发投入资本化的金额（元）'] = split_kongge(list[i][2])
                            dict_temp2['研发投入资本化的金额（元）'] = split_kongge(list[i][3])
                            dict_temp3['研发投入资本化的金额（元）'] = split_kongge(list[i][4])
                            continue
                        dict_temp1[split_kongge(list[i][0])] = split_kongge(list[i][1])
                        dict_temp2[split_kongge(list[i][0])] = split_kongge(list[i][2])
                        dict_temp3[split_kongge(list[i][0])] = split_kongge(list[i][3])
                dict = [dict_temp1, dict_temp2, dict_temp3]
            else:
                for i in range(len(list)):
                    dict_temp1[split_kongge(list[i][0])] = split_kongge(list[i][1])
                    dict_temp2[split_kongge(list[i][0])] = split_kongge(list[i][2])
                    dict_temp3[split_kongge(list[i][0])] = split_kongge(list[i][3])
                dict = [dict_temp1, dict_temp2, dict_temp3]

     

    print(dict)
    return dict


write_to_dict_6_2(1, yftr, '2019')
