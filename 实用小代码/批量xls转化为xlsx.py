import os
import win32com.client as win32

def save_as_xlsx(fname):
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(fname)
    wb.SaveAs(fname+"x", FileFormat = 51)    #FileFormat = 51 is for .xlsx extension
    wb.Close()                               #FileFormat = 56 is for .xls extension
    excel.Application.Quit()

if __name__ == "__main__":
    package = "E:/11111-工作内容/BBB-数据入库工作/中国工业企业数据库/2013年"
    files = os.listdir(package)
    for fname in files:
        print(fname)
        if fname.endswith(".xls"):
            print(fname + "正在进行格式转换中")
        	# print(fname + "正在进行格式转换，请稍后~")
            save_as_xlsx(package + "/"+ fname)
            print(fname + "格式转换完成")
        else:
            print("跳过非xls文件："+fname)