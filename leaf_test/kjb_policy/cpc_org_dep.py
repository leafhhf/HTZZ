from selenium import webdriver
import time
from selenium.webdriver.chrome.options import Options
import pandas as pd


chrome_options =Options()
chrome_options.add_argument('--headless')







# browser = webdriver.Chrome(options=chrome_options)
browser = webdriver.Chrome()
browser.get('https://www.12371.cn/special/zcwj/')
# year_list = ["2021年","2020年","2019年","2018年","2017年","2016年","2015年","2014年","2013年","2012年"]


url_list = []
crawl_year = []
trlist_all = []

####2021年
browser.find_element_by_xpath("//a[contains(text(),'2021年')]").click()
trlist = browser.find_elements_by_xpath("//div[@id='one_upELMT001']/li/div/a")
for tr in trlist:
    trlist_all.append(tr)
    crawl_year.append("2020年")
for a in trlist:
    url_list.append(a.get_attribute('href'))
# 尝试翻页
while True:
    try :
        browser.find_element_by_xpath("//a[contains(text(),'下一页')]").click()
        trlist = browser.find_elements_by_xpath("//div[@id='one_upELMT001']/li/div/a")
        print("已读取下一页内容")
        for tr in trlist:
            trlist_all.append(tr)
            crawl_year.append("2020年")
        for a in trlist:
            url_list.append(a.get_attribute('href'))
    except:
        print("没有下一页")
        break






###2020年
browser = webdriver.Chrome()
browser.get('https://www.12371.cn/special/zcwj/')
browser.find_element_by_xpath("//a[contains(text(),'2020年')]").click()
trlist = browser.find_elements_by_xpath("//div[@id='one_upELMT001']/li/div/a")
for tr in trlist:
    trlist_all.append(tr)
    crawl_year.append("2020年")
for a in trlist:
    url_list.append(a.get_attribute('href'))
# 尝试翻页
while True:
    try :
        browser.find_element_by_xpath("//a[contains(text(),'下一页')]").click()
        trlist = browser.find_elements_by_xpath("//div[@id='one_upELMT001']/li/div/a")
        print("已读取下一页内容")
        for tr in trlist:
            trlist_all.append(tr)
            crawl_year.append("2020年")
        for a in trlist:
            url_list.append(a.get_attribute('href'))
    except:
        print("没有下一页")
        break





title = []

zw=[]
text = []
public_date = []
for url in url_list:
    print(url)
    driver = webdriver.Chrome(options=chrome_options)
    driver.get('{}'.format(url))
    title_list = driver.find_elements_by_xpath("//h1[@class='big_title']")
    title.append(title_list[0].text)
    text_list = driver.find_elements_by_xpath("//div[@class='word']/p")
    for t in text_list:
        zw.append(t.text)
    text.append("\n".join(zw))
    date_list = driver.find_element_by_xpath("//i[@class='time']")
    date = date_list.text
    time1 = date[date.find("发布时间：")+5: date.find("日")+1]
    time1 =time.strptime(time1, "%Y年%m月%d日")
    public_date.append(time.strftime("%Y-%m-%d %H:%M:%S", time1))

df = pd.DataFrame


for url in url_list:
    print(url)