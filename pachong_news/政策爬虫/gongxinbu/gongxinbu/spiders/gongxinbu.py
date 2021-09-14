# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from gongxinbu.items import GongxinbuItem
import json
import datetime
import time
from scrapy.spiders import CrawlSpider, Rule
import hashlib
import requests
from urllib.parse import urljoin
import os

class FaGaiWei(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "gongxinbu"
    # 指定爬虫入口，scrapy支持多入口，所以一定是lis形式
    headers = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        # 这里必须有，才会有下面的解析方式
        'Host':'www.miit.gov.cn',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://www.miit.gov.cn/search/wjfb.html?websiteid=110000000000000&pg=&p=&tpl=14&category=51&q=',
        'Connection': 'keep-alive',
        'Cookie':'jsessionid=rBQTGhroX9rIRPlvzX6S2ETpoCU9ihzU3X8A; Hm_lvt_af6f1f256bb28e610b1fc64e6b1a7613=1598410762,1598410795,1598410810,1598411785; SF_cookie_1=31501425; wzws_cid=1bc828bf9a6da5d18529f90a7156e4d5d50b346541678a63a774a493baa70641b7908518aeb66fd04031a35abca6ea4e5a86536b180ba49803def7f44aebec46c746c7359f5dfb4bd24135929c4bd60f',
    }

    def start_requests(self):
        currenturl = 'https://www.miit.gov.cn/search-front-server/api/search/info?websiteid=110000000000000&scope=basic&q=&pg=10&cateid=57&level=6&p=1&begin=2018-01-01'
        response = requests.get(currenturl, headers=self.headers)
        totalCount = json.loads(response.text)["data"]["searchResult"]
        pagenum = totalCount['total']
        num = int(pagenum/10+1)
        print('num',num)
        for i in range(1,num):
           # url = 'https://www.ofweek.com/ai/CATList-201700-{page}-30436968,30436767,30436352,30436166,30435983,30435786,30435530.htm'.format(page=i)
           # url='https://www.miit.gov.cn/search-front-server/api/search/info?websiteid=110000000000000&scope=basic&q=&pg=10&cateid=57&pos=title_text,infocontent,titlepy&_cus_eq_typename=&_cus_eq_publishgroupname=&_cus_eq_themename=&begin=2018-01-01&end=2020-12-17&dateField=deploytime&selectFields=title,content,deploytime,_index,url,cdate,infoextends,infocontentattribute,columnname,filenumbername,publishgroupname,publishtime,metaid,bexxgk,columnid,xxgkextend1,xxgkextend2,themename,typename,indexcode,createdate&group=distinct&highlightConfigs=[{%22field%22:%22infocontent%22,%22numberOfFragments%22:2,%22fragmentOffset%22:0,%22fragmentSize%22:30,%22noMatchSize%22:145}]&highlightFields=title_text,infocontent,webid&level=6&sortFields=[{%22name%22:%22deploytime%22,%22type%22:%22desc%22}]&p=｛page｝'.format(page=i)
            url='https://www.miit.gov.cn/search-front-server/api/search/info?websiteid=110000000000000&scope=basic&q=&pg=10&cateid=57&level=6&p={page}&begin=2018-01-01&selectFields=title,title_text,content,deploytime,_index,url,cdate,columnname,filenumbername,publishgroupname,publishtime,metaid,bexxgk,columnid,xxgkextend1,xxgkextend2,themename,typename,indexcode,createdate,infocontent'.format(page=i)

            print(url)
            yield scrapy.Request(url=url, callback=self.parse,headers=self.headers)
    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")
        searchResult=json.loads(response.text)["data"]["searchResult"]

        dataResults=searchResult["dataResults"]
       # print('dataResults', dataResults)
       # print('data',data)
        for i in dataResults:
            item = GongxinbuItem()
            item["title"] = i["data"]["title_text"]


           # item["publishTime"] = i["data"]['publishtime']  ##发布日期

            item["publishTime"] = i["data"]['jsearch_date']  ##发布日期
            item["columnname"] = i["data"]['columnname'] ##公文种类
            item["url"] = i["data"]['url']  ##url
            item["content"] = i["data"]['infocontent']  ##内容
            item["url"]=response.urljoin(item["url"])

            if "xxgkextend2" in i["data"] :
                item["puborg"] = i["data"]["xxgkextend2"]  ##发文机关
            else:
                item["puborg"]=''
            if "themename" in i["data"]:
                item["themename"] = i["data"]["themename"]  ##主题分类
            else:
                item["themename"] = ''
            if "filenumbername" in i["data"]:
                item["filenumbername"] = i["data"]["filenumbername"]  ##发文字号
            else:
                item["filenumbername"] = ''
            if "publishgroupname" in i["data"]:
                item["pubjigou"] = i["data"]["publishgroupname"]  ##发布机构
            else:
                item["pubjigou"] = ''
            if "createdate" in i["data"]:
                item["createdate"] = i["data"]["createdate"]  ##成文日期
                timeArray = time.localtime(int(item["createdate"][0:10]))
                item["createdate"] = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            else:
                item["createdate"] = ''


            print('打印', item)
            yield item
