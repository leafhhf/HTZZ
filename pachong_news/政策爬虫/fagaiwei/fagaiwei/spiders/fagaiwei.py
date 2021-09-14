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
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        # 这里必须有，才会有下面的解析方式
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://www.ndrc.gov.cn/xxgk/wjk/index.html?tab=all&date=2020',
        'Connection': 'keep-alive',
    }

    def start_requests(self):
        for i in range(51,70):
           # url = 'https://www.ofweek.com/ai/CATList-201700-{page}-30436968,30436767,30436352,30436166,30435983,30435786,30435530.htm'.format(page=i)
            url='https://so.ndrc.gov.cn/api/query?qt=&tab=all&page={page}&pageSize=20&siteCode=bm04000fgk&key=CAB549A94CF659904A7D6B0E8FC8A7E9&startDateStr=2018-01-01%2000:00:00&endDateStr=all-12-31%2023:59:59&timeOption=0&sort=dateDesc'.format(page=i)
            print(url)
            yield scrapy.Request(url=url, callback=self.parse,headers=self.headers)
    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")

        jsonlist = json.loads(response.text)
        data = jsonlist.get('resultList')
        for i in data:
            item = FagaiweiItem()
            item["title"] = i['title']
            item["url"] = i['url']
            item["publishTime"] = i['publishTime']
            yield scrapy.Request(item["url"],meta={'item': item},callback=self.parse_detail )

    def parse_detail(self, response):  # 处理详情页

        item = response.meta['item']
        zw = []
       # content_img = []
        content1 = response.xpath("//div[@class='article_l']//span")
        content2 = response.xpath("//div[@class='article_l']//p")
        content =content1+content2

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
