# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from cnmoii.items import CnmoiiItem
#from ai_news.settings import JUZI_PWD, JUZI_USER
import json
import datetime
import time
from scrapy.spiders import CrawlSpider, Rule
import hashlib
import requests
import os


class Cnmoii(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "cnmoii"
    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        # 这里必须有，才会有下面的解析方式
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'http://www.cnmoii.com/zixun/index.html',
        'Connection': 'keep-alive',

    }
    # 指定爬虫入口，scrapy支持多入口，所以一定是lis形式
    def start_requests(self):
        #'http://www.cnmoii.com/zixun/'
        url = 'http://www.cnmoii.com/zixun/index.html'
        #for i in range(2,15):
        #   url = 'http://www.cnmoii.com/zixun/index_{page}.html'.format(page=i)
      #      print('url',url)
        yield scrapy.Request(url=url, callback=self.parse,headers=self.headers)

    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")
        trlist = response.xpath("//article[@class='category-blog cate11 auth1']")
       # print('trlist', trlist)
        for tr in trlist:
            print('tr', tr)
            item = CnmoiiItem()
            item['title'] = tr.xpath("./h2/a/@title").extract_first()
            item['url'] = tr.xpath("./h2/a/@href").extract_first()
            item['imgurl'] = tr.xpath("./div/a/img/@src").extract_first()
            item['summary'] = tr.xpath("./div[@class='intro']/p/text()").extract_first().strip()
            #item['reporttime'] = tr.xpath(".//div[@class='box-right-text-left' or @class='text-bottom mt20' or @class='text-bottom mt15']/span[5]/text()").extract_first().strip()
           # item['source'] = '人工智能网'
            print("item['title']",item['title'])
            yield scrapy.Request(item["url"],
                 callback=self.parse_detail,
                 meta={"item": item},
    )
    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]
      #  item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
        item["reporttime"] = response.xpath("//div[@class='postmeta']/span[2]/text()").extract_first().replace("\n","").replace("\r","").replace("\t","").strip().replace("时间：","")
       #item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first().replace("\n","").replace("\r","")

        zw = []
        content_img = []
        content = response.xpath("//div[@class='entry'][1]/p")
        for cn in content:
           # item["content"] = cn.xpath("./p").extract_first()
            img = cn.xpath('.//img/@src')
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
        print('content',item["content"])
        item["content"] ="\n" .join(item["content"])
        item["content_img"] = ",".join(item["content_img"])
        print('打印',item)
        yield item
            # 将这一级提取到的信息，通过请求头传递给下一级（这里是为了给数据打标签
