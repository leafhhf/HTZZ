






# encoding: utf-8
import os
import sys
import requests
import time
import tkinter as tk
from tkinter import filedialog
from aip import AipOcr

# 定义常量
APP_ID = '24760099'
API_KEY = 'nnwUzFumaZO9oiVX18pfts0b'
SECRET_KEY = 'E8PMv3dqOjyPBZoproTvY7WldBzK65Nj'
# 初始化AipFace对象
client = AipOcr(APP_ID, API_KEY, SECRET_KEY)

# 读取图片
def get_file_content(filePath):
    with open(filePath, 'rb') as fp:
        return fp.read()


#文件下载函数
def file_download(url, file_path):
    r = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(r.content)


if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw()
    data_dir = r'C:\Users\Administrator\Desktop\python_work\独角兽名单'
    result_dir = r'C:\Users\Administrator\Desktop\python_work\独角兽名单\result'

    # data_dir = filedialog.askdirectory(title='请选择图片文件夹') + '/'
    # result_dir = filedialog.askdirectory(title='请选择输出文件夹') + '/'
    num = 0
    for name in os.listdir(data_dir):
        print ('{0} : {1} 正在处理：'.format(num+1, name.split('.')[0]))
        image = get_file_content(os.path.join(data_dir, name))
        res = client.tableRecognitionAsync(image)
        # print ("res:", res)
        if 'error_code' in res.keys():
            print ('Error! error_code: ', res['error_code'])
            sys.exit()
        req_id = res['result'][0]['request_id']    #获取识别ID号

        for count in range(1, 20):    #OCR识别也需要一定时间，设定10秒内每隔1秒查询一次
            res = client.getTableRecognitionResult(req_id)    #通过ID获取表格文件XLS地址
            print(res['result']['ret_msg'])
            if res['result']['ret_msg'] == '已完成':
                break    #云端处理完毕，成功获取表格文件下载地址，跳出循环
            else:
                time.sleep(1)

        url = res['result']['result_data']
        xls_name = name.split('.')[0] + '.xls'
        file_download(url, os.path.join(result_dir, xls_name))
        num += 1
        print ('{0} : {1} 下载完成。'.format(num, xls_name))
        time.sleep(1)

## excel文件合并成csv
import pandas as pd
import os
filepath = r"C:\Users\Administrator\Desktop\python_work\独角兽名单\result"
filenames = os.listdir(filepath)

# print(filenames)
num = 0

dfs = []
for name in filenames:
    print(f'第{num}个文件正在合并中')
#     print(path +"\\"+name)
    dfs.append(pd.read_excel(os.path.join(filepath,name),sheet_name="body"))
    num += 1    #为了查看合并到第几个表格了


df = pd.concat(dfs)

df.to_csv(r"C:\Users\Administrator\Desktop\python_work\独角兽名单\result\all.csv",  index=False, header=False)

#writer=pd.ExcelWriter(r"E:\11111-工作内容\AAA-科技部\全球上市公司数据\原表\combine.csv")
#df.to_csv(writer,sheet_name='Data1',index=False)

#writer.save()

print('done')