import pdfplumber
import os
import xlrd
import re
import json


def select(and(delete(list):
    new(list = []
    for i in range(len(list)):
        new(list(temp = []
        for j in range(len(list[i])):
            if list[i][j] is None or len(list[i][j]) == 0:
                continue
            else:
                new(list(temp.append(list[i][j])
        new(list.append(new(list(temp)

    return new(list


def select(page(list):
    list(select = []
    if len(list) == 1:
        return list

    for i in range(len(list)):
        if i == 0:
            list(select.append(list[i])
        elif (list[i] - list[i - 1]) > 1:
            continue
        else:
            list(select.append(list[i])
    return list(select


def split(kongge(s):
    if isinstance(s, str):

        return "".join((re.sub("\n", " ", s)).split(" "))
    else:
        return s


def split(kongge(list(list):
    new(list = []
    for i in range(len(list)):
        new(list(temp = []
        for j in range(len(list[i])):

            if isinstance(list[i][j], str):
                new(list(temp.append("".join((re.sub("\n", " ", list[i][j])).split(" ")))
            else:
                new(list(temp.append(list[i][j])
        new(list.append(new(list(temp)

    return new(list
# 删除list中带有‘’或者None的字段
def delete(list(list):
    new(list = []
    for i in range(len(list)):
        new(list(temp = []
        for j in range(len(list[i])):
            if list[i][j] is None:
                new(list(temp.append(list[i][j])
                continue
            if len(list[i][j]) == 0:
                continue
            else:
                new(list(temp.append(list[i][j])
        new(list.append(new(list(temp)

    return new(list

# 将PDF内容全部提取
def parse(pages(file(path):
    try:
        pages = []
        pdf = pdfplumber.open(file(path)
        print('parse file:{}   page num:{}'.format(os.path.basename(file(path), len(pdf.pages)))
        for index, page in enumerate(pdf.pages):
            if index == 50:
                print('index',index)
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
table(header(yftr = ['研发投入金额（元）', '研发人员数量（人）', '资本化研发支出占当期净利', '资本化研发支出占研发投入', '资本化研发投入占研发投入']


# 获取解析字段所在的页码
def get(page(table, pages, list):
    table = split(kongge(list(table)

    for i in range(len(table)):
        if table(header(yftr[1] in table[i] or table(header(yftr[0] in table[i]:
            if pages in list:
                continue
            else:
                list.append(pages)

    for i in range(len(table)):

        if table(header(yftr[2] in table[i] or table(header(yftr[3] in table[i] or table(header(yftr[4] in table[i]:
            # print(1)
            # print(pages)
            if pages in list:
                continue
            else:
                list.append(pages)


# 获取解析的信息
def get(information(list(page, tables, list, pages):
    # list表示页码范围
    # tables表示解析的表格
    # map表示要存储的映射
    # pages 表示页码
    # index( 表示table(header中对应的索引
    if len(list(page) == 1:
       # print('list(page[0]',list(page[0])
       # print('pages',pages)
        if pages == list(page[0]:
           
            for index, p in enumerate(tables):
                p = delete(list(p)
               # print('p[0]',p)
               # print('p[1]',p[4][1])
                if len(p) == 1:
                     continue
                #if (p[0][0] is None) or (p[1][0] is None):
                    #continue
               # print('min',min(len(p),3))
                
                for i in range(len(p)):
                    #print(i,p[i])
                                     
                    
                    for j in range(min(len(p[i]),3)):
                        
                        if p[i][j] is None or p[i][j] == '':
                            continue
                        #print('p[i][j]',p[i][j])
                        if table(header(yftr[1] in p[i][j] or table(header(yftr[1] in p[i][j]:
               # if table(header(yftr[1] in p[0][0] or table(header(yftr[1] in p[1][0] or table(header(yftr[0] in p[0][0] or table(header(yftr[0] in p[1][0]:
                            list.extend(p)
                    #print('pppppppppp',p)
    elif len(list(page) == 2:
        if pages == list(page[0]:
            for index, p in enumerate(tables):
                p = delete(list(p)
                #print(p)
                if len(p) == 1:
                    continue
                for i in range(len(p)):
                    #print(i,p[i])
                                     
                    
                    for j in range(min(len(p[i]),3)):
                        
                        if p[i][j] is None or p[i][j] == '':
                            continue
                        #print('p[i][j]',p[i][j])
                        if table(header(yftr[1] in p[i][j] or table(header(yftr[1] in p[i][j]:
                            list.extend(p)
               # if (p[0][0] is None) or (p[1][0] is None):
               #     continue
                #if table(header(yftr[1] in p[0][0] or table(header(yftr[1] in p[1][0] or table(header(yftr[0] in p[0][0] or table(header(yftr[0] in p[1][0]:
                   # list.extend(p)
        elif pages == list(page[1]:
            for index, p in enumerate(tables):
                p = delete(list(p)
                if index == 0:
                    list.extend(p)


def match(yftr2(all(pages):
    list(page(yftr = []

    for index, page in enumerate(all(pages):
        tables = page['tables']
        pages = page['page']
        # if pages == 18:
        #     print(tables)

        for index2, table in enumerate(tables):
            get(page(table, pages, list(page(yftr)
    #
    # print('list(page(yftr',list(page(yftr)
    # yftr = {}
    #
    # list(zcqk = []
    #
    #
    yftr = []
    #
    for index, page in enumerate(all(pages):
        if index == 50:
            break
        tables = page['tables']
        pages = page['page']
        #if pages == 17:
        #    print(tables)
       # print('table',tables)
       # print(list(page(yftr)
        get(information(list(page(yftr, tables, yftr, pages)
    print(yftr)
    return yftr


# 更换需要解析的pdf的相对路径即可
#pages = parse(pages('PDF/test/美好置业(000667.SZ)(2019-02-28.pdf')
pages = parse(pages('PDF/test/方正电机(002196.SZ)(2019-04-29.pdf')
#
yftr = match(yftr2(pages)

print('yftr',yftr)

#
def hebing(list):
    new(list = []
    temp = 0
    for i in range(len(list)):

        if i == temp:
            # print(list[i][0])
            if list[i][0] is None:
                list(temp = []
                # if list[i - 1][0] == '资本化研发投入占研发投入的\n比例':
                #
                if list[i + 1][0] is None:
                    list(temp.append(list[i - 1][0] + list[i + 1][1])
                    list(temp.append(list[i][3])
                    list(temp.append(list[i][4])
                    list(temp.append(list[i][5])
                else:
                    list(temp.append(list[i - 1][0] + list[i + 1][0])
                    list(temp.append(list[i][1])
                    list(temp.append(list[i][2])
                    list(temp.append(list[i][3])

                new(list.remove(new(list[-1])
                new(list.append(list(temp)
                temp = i + 3
            else:
                new(list.append(list[i])
                temp = i + 1
        else:
            continue
    return new(list


def write(to(dict(6(2(result(dir, list, time):
   # if not os.path.exists(result(dir):
   #    os.makedirs(result(dir)
    print('前',list)
    list = hebing(list)
    print('后',list)

    dict = []
    #print('list[0][0]',list[0][0])
    #print('list',list)
    #list=select(and(delete(list)
   
    list=[x for x in list if x!=[]]
    print('list(new',list)
   # if list[0][0] == '':
    dict(temp = {}
    dict(temp['date'] = str(int(time)-1)
    for i in range(len(list)):
       #  print('list',list[i])
         #   if i == 0:
          #      dict(temp['date'] = split(kongge(list[i][1])
         if i >= 1:
            dict(temp[split(kongge(list[i][0])] = split(kongge(list[i][1])

  #  elif list[0][0] != '':
   #     dict(temp = {}
    #    dict(temp['date'] = str(int(time))
    #    for i in range(len(list)):
    #        dict(temp[split(kongge(list[i][0])] = split(kongge(list[i][1])
        # if '%' in list[1][-1]:
        #
        #     dict(temp1 = {}
        #     dict(temp2 = {}
        #     dict(temp1['date'] = str(int(time))
        #     dict(temp2['date'] = str(int(time) - 1)
        #     for i in range(len(list)):
        #         dict(temp1[split(kongge(list[i][0])] = split(kongge(list[i][1])
        #         dict(temp2[split(kongge(list[i][0])] = split(kongge(list[i][2])
        #     dict = [dict(temp1, dict(temp2]
        #
        # else:
        #     dict(temp1 = {}
        #     dict(temp2 = {}
        #     dict(temp3 = {}
        #     dict(temp1['date'] = str(int(time))
        #     dict(temp2['date'] = str(int(time) - 1)
        #     dict(temp3['date'] = str(int(time) - 2)
        #     for i in range(len(list)):
        #         dict(temp1[split(kongge(list[i][0])] = split(kongge(list[i][1])
        #         dict(temp2[split(kongge(list[i][0])] = split(kongge(list[i][2])
        #         dict(temp3[split(kongge(list[i][0])] = split(kongge(list[i][3])
        #     dict = [dict(temp1, dict(temp2, dict(temp3]

    print(dict(temp)
    return dict(temp


write(to(dict(6(2(1, yftr, '2019')
