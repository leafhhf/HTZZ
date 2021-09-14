# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from ainews.items import AInewsItem
#from ai_news.settings import JUZI_PWD, JUZI_USER
import json
import datetime
import time
from scrapy.spiders import CrawlSpider, Rule
import hashlib
import requests
import os

class AiNews(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "ainews"
    # 指定爬虫入口，scrapy支持多入口，所以一定是lis形式
    headers = {

        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        # 这里必须有，才会有下面的解析方式
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://www.ofweek.com/ai/',
        'Connection': 'keep-alive',
    }

    def start_requests(self):
        for i in range(1,50):
            url = 'https://www.ofweek.com/ai/CATList-201700-{page}-30436968,30436767,30436352,30436166,30435983,30435786,30435530.htm'.format(page=i)
            print(url)
            yield scrapy.Request(url=url, callback=self.parse,headers=self.headers)


    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")

        jsonlist = json.loads(response.text)
        data = jsonlist.get('newsList')
        for i in data:
            item = AInewsItem()
            item["title"] = i['title']
            item["url"] = i['htmlpath']
            item["summery"] = i['summery']
            item["type"] = i['channelStr']
            item["reporttime"] = i['intervalStr']
            item["keyword"] = i['metakeywords']
            yield scrapy.Request(item["url"],meta={'item': item},callback=self.parse_detail )

    def parse_detail(self, response):  # 处理详情页

        item = response.meta['item']
        print('item',item["title"])
       # item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
       # item["reporttime"] = response.xpath("//div[@class='time fl']/text()").extract_first().replace("\n","").replace("\r","").replace("\t","")
        item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first()
        #if item["source"]:
            #item["source"]=item["source"].replace("\n","").replace("\r","")
        zw = []
        content_img = []
        zw_content = response.xpath("//div[@class='artical-content']")
        content = response.xpath("//div[@class='artical-content']/p")
        for cn in content:
           # item["content"] = cn.xpath("./p").extract_first()
            img = cn.xpath('./img/@src')
            text = cn.xpath('.//text()').extract()
            text = "".join(text)
            if img:
                zw.append('#img#')
                imgurl = response.urljoin(img.extract_first())
                #content_img.append(imgurl)
                content_img.append(imgurl)
            #if text and text.strip() != '\n':
            if text:

                zw.append(text.strip('\n'))
                #  zw.append(text)
        item["content"] = zw

        item["content_img"] = content_img

       # item["content"] = response.xpath("//div[@class='artical-content'][1]").extract_first()
       # print('1111111111item["content"] ',item["content"] )
      #  print('content',item["content"])
        item["content"] ="\n" .join(item["content"])
        item["content_img"] = ",".join(item["content_img"])
           # print('打印',item)
        yield item
            # 将这一级提取到的信息，通过请求头传递给下一级（这里是为了给数据打标签
