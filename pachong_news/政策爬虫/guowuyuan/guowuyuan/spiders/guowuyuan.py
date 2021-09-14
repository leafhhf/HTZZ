# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from guowuyuan.items import GuowuyuanItem
import json
import datetime
import time
from scrapy.spiders import CrawlSpider, Rule
import hashlib
import requests
import os

class FaGaiWei(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "guowuyuan"
    # 指定爬虫入口，scrapy支持多入口，所以一定是lis形式
    headers = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        # 这里必须有，才会有下面的解析方式
        'Host':'sousuo.gov.cn',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'http://sousuo.gov.cn/s.htm?q=&t=zhengcelibrary&orpro=',
        'Connection': 'keep-alive',
        'Cookie':'allmobilize=mobile; wdcid=329af9abbfcde1bc; wdlast=1608098204; wdses=4465543e16546a83; _gscu_603879440=08079724mgcdyp21; _gscbrs_603879440=1',
    }

    def start_requests(self):
        currenturl = 'http://sousuo.gov.cn/data?t=zhengcelibrary_bm&q=&timetype=timezd&mintime=2018-01-01&maxtime=2020-12-16&searchfield=title:content:summary&pcodeJiguan=&puborg=&pcodeYear=&pcodeNum=&filetype=&p=0&n=5&sort=score&bmfl=&bmpubyear='
        response = requests.get(currenturl, headers=self.headers)
        totalCount = json.loads(response.text)["searchVO"]
        pagenum = totalCount['totalCount']
        num = int(pagenum/5+1)
        print('num',num)
        for i in range(120,num):
           # url = 'https://www.ofweek.com/ai/CATList-201700-{page}-30436968,30436767,30436352,30436166,30435983,30435786,30435530.htm'.format(page=i)
            url='http://sousuo.gov.cn/data?t=zhengcelibrary_bm&q=&timetype=timezd&mintime=2018-01-01&maxtime=2020-12-16&searchfield=title:content:summary&pcodeJiguan=&puborg=&pcodeYear=&pcodeNum=&filetype=&p={page}&n=5&sort=score&bmfl=&bmpubyear='.format(page=i-1)
            print(url)
            yield scrapy.Request(url=url, callback=self.parse,headers=self.headers)
    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")
        totalCount=json.loads(response.text)["searchVO"]

        data = json.loads(response.text)["searchVO"]
        gongwen=data["listVO"]
      #  print('data',gongwen)
        for i in gongwen:
            item = GuowuyuanItem()
            item["title"] = i['title']
            item["publishTime"] = i['pubtimeStr']
            item["summary"] = i['summary']
            item["url"] = i['url']
            item["pcode"] = i['pcode']
            item["ptime"] = i['ptime']
            item["source"] = i['source']
            item["childtype"] = i['childtype']
            item["puborg"] = i['puborg']
            item["subjectword"] = i['subjectword']
            item["colname"] = i['colname']

            yield scrapy.Request(item["url"],meta={'item': item},callback=self.parse_detail )

    def parse_detail(self, response):  # 处理详情页

        item = response.meta['item']
        zw = []
       # content_img = []
        content1 = response.xpath("//td[@id='UCAP-CONTENT']//p")
        content2 = response.xpath("//td[@id='UCAP-CONTENT']//span")
        content =content1+content2

        for cn in content:
           # item["content"] = cn.xpath("./p").extract_first()
        #    img = cn.xpath('./img/@src')
            text = cn.xpath('.//text()').extract()
            text = "".join(text)

            if text:
                zw.append(text.strip('\n'))
                #  zw.append(text)
        zw_all = zw
       # item["content"] = response.xpath("//div[@class='artical-content'][1]").extract_first()
       # print('1111111111item["content"] ',item["content"] )
      #  print('content',content2)
       # print(zw_all)
        item["content"] = list(set(zw_all))
        item["content"].sort(key=zw_all.index)
    #
        item["content"] ="\n" .join(item["content"])

      #  print('打印',item)
        yield item
            # 将这一级提取到的信息，通过请求头传递给下一级（这里是为了给数据打标签
