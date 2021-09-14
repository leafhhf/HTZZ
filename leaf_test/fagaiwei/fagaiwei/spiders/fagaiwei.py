# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from fagaiwei.items import FagaiweiItem
import json
import datetime
import time
from scrapy.spiders import CrawlSpider, Rule
import hashlib
import requests
import os

class FaGaiWei(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "fagaiwei"
    # 指定爬虫入口，scrapy支持多入口，所以一定是lis形式
    headers = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        # 这里必须有，才会有下面的解析方式
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.ndrc.gov.cn/',
        'Connection': 'keep-alive',
        'Host':'fwfx.ndrc.gov.cn',
    }

    def start_requests(self):
        for i in range(1,5):
            url='https://fwfx.ndrc.gov.cn/api/query?qt=&tab=all&page={page}&pageSize=20&siteCode=bm04000fgk&key=CAB549A94CF659904A7D6B0E8FC8A7E9&startDateStr=2021-01-01&endDateStr=2021-12-31&timeOption=2&sort=dateDesc'.format(page=i)
            print(url)
            yield scrapy.Request(url=url, callback=self.parse,headers=self.headers)

    def parse(self, response):
        #
        #now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        #str_YMD = now_time.strftime("%Y-%m-%d")
        jsonlist = json.loads(response.text)
        # print(jsonlist)
        json_data = jsonlist.get("data").get('resultList')
        print(json_data)
        for i in json_data:
            item = FagaiweiItem()
            item["title"] = i['dreTitle']
            item["url"] = i['url']
            item["publishTime"] = i['docDate']
            yield scrapy.Request(item["url"],meta={'item': item},callback=self.parse_detail )


    def parse_detail(self, response):  # 处理详情页
        item = response.meta['item']
        zw = []
       # content_img = []
        content1 = response.xpath("//div[@class='article_l']//span")
        content2 = response.xpath("//div[@class='article_l']//p")
        content= content1 + content2
        for cn in content:
           # item["content"] = cn.xpath("./p").extract_first()
        #    img = cn.xpath('./img/@src')
            text = cn.xpath('.//text()').extract()
            text = "".join(text)

            if text:
                zw.append(text.strip('\n'))
                #  zw.append(text)
        content2 = zw
       # item["content"] = response.xpath("//div[@class='artical-content'][1]").extract_first()
       # print('1111111111item["content"] ',item["content"] )
      #  print('content',content2)
        item["content"] = list(set(content2))
        item["content"].sort(key=content2.index)
    #    print(item["content"])
        item["content"] ="\n" .join(item["content"])
     #   print('打印',item)
        yield item
            # 将这一级提取到的信息，通过请求头传递给下一级（这里是为了给数据打标签




