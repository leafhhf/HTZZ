# -*- coding: utf-8 -*-

# -------------------------------多个excel的单个sheet的合并----------------------
import xlrd
import xlsxwriter
import os

# 打开一个excel文件
def open_xls(file):
    fh = xlrd.open_workbook(file)
    return fh

# 获取excel中所有的sheet表
def getsheet(fh):
    return fh.sheet_by_name('2013')

# 获取sheet表的行数
# def getnrows(fh, sheet):
#     table = fh.sheets()[sheet]
#     return table.nrows

# 读取文件内容并返回行内容
def getFilect(file):
    fh = open_xls(file)
    table = fh.sheet_by_name('2013')
    # table = fh.sheets()[shnum]
    num = table.nrows
    for row in range(num):
        rdata = table.row_values(row)
        datavalue.append(rdata)
    return datavalue


if __name__ == '__main__':
    # 定义要合并的excel文件列表
    # allxls = ['F:/test/excel1.xlsx', 'F:/test/excel2.xlsx']
    filepath = "E:/11111-工作内容/BBB-数据入库工作/中国工业企业数据库/2013年/转化后"
    filenames = os.listdir(filepath)
    allxls =[]
    for name in filenames:
        allxls.append(filepath+"/"+name)
    print(allxls)

    # 存储所有读取的结果
    datavalue = []
    for fl in allxls:
        fh = open_xls(fl)
        x = getsheet(fh)
        rvalue = getFilect(fl)
    # 定义最终合并后生成的新文件
    endfile = 'E:/11111-工作内容/BBB-数据入库工作/中国工业企业数据库/2013年/转化后/all.xls'
    wb1 = xlsxwriter.Workbook(endfile)
    # 创建一个sheet工作对象
    ws = wb1.add_worksheet()
    for a in range(len(rvalue)):
        for b in range(len(rvalue[a])):
            c = rvalue[a][b]
            ws.write(a, b, c)
    wb1.close()
    print("文件合并完成")





# -------------------------------------多个excel文件且多个sheet的合并-------
# 将多个Excel文件合并成一个
import xlrd
import xlsxwriter
import os

# 打开一个excel文件
def open_xls(file):
    fh = xlrd.open_workbook(file)
    return fh

# 获取excel中所有的sheet表
def getsheet(fh):
    return fh.sheets()

# 获取sheet表的行数
# def getnrows(fh, sheet):
#     table = fh.sheets()[sheet]
#     return table.nrows

# 读取文件内容并返回行内容
def getFilect(file, shnum):
    fh = open_xls(file)
    table = fh.sheets()[shnum]
    num = table.nrows
    for row in range(num):
        rdata = table.row_values(row)
        datavalue.append(rdata)
    return datavalue

# 获取sheet表的个数
def getshnum(fh):
    x = 0
    sh = getsheet(fh)
    for sheet in sh:
        x += 1
    return x

if __name__ == '__main__':
    # 定义要合并的excel文件列表
    # allxls = ['F:/test/excel1.xlsx', 'F:/test/excel2.xlsx']
    filepath = "C:/Users/Administrator/Desktop/python_work/独角兽名单/result/xls"
    filenames = os.listdir(filepath)
    allxls =[]
    for name in filenames:
        allxls.append(filepath+"/"+name)
    print(allxls)

    # 存储所有读取的结果
    datavalue = []
    for fl in allxls:
        fh = open_xls(fl)
        x = getshnum(fh)
        for shnum in range(x):
            print("正在读取文件：" + str(fl) + "的第" + str(shnum) + "个sheet表的内容...")
            rvalue = getFilect(fl, shnum)
    # 定义最终合并后生成的新文件
    endfile = 'C:/Users/Administrator/Desktop/python_work/独角兽名单/result/xls/all.xls'
    wb1 = xlsxwriter.Workbook(endfile)
    # 创建一个sheet工作对象
    ws = wb1.add_worksheet()
    for a in range(len(rvalue)):
        for b in range(len(rvalue[a])):
            c = rvalue[a][b]
            ws.write(a, b, c)
    wb1.close()
    print("文件合并完成")

