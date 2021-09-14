# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from lianmenhu.items import LianmenhuItem
#from ai_news.settings import JUZI_PWD, JUZI_USER
import json
import datetime
import time


class Lianmenhu(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "lianmenhu"
    domain = "lianmenhu.com"
    # 指定爬虫入口，scrapy支持多入口，所以一定是lis形式
    #start_urls = ['http://www.lianmenhu.com/apply/list-1','http://www.lianmenhu.com/ecology/list-1']
    start_urls = ['http://www.lianmenhu.com/ecology/list-1']
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        'Host': 'www.lianmenhu.com',
        'Connection': 'keep-alive',
        'Cookie': 'Hm_lvt_46cc93c2f6ffd045047c9b26f3870f42=1588919529,1590372379; a615_0a47_saltkey=st5DHezs; a615_0a47_lastvisit=1589007294; a615_0a47_sid=JKZ1KT; a615_0a47_lastact=1590375590%09index.php%09; Hm_lpvt_46cc93c2f6ffd045047c9b26f3870f42=1590375559; a615_0a47_wq_article=wqid_3243; a615_0a47_sendmail=1',
       # 'Referer': 'http://www.lianmenhu.com/apply/',
        'Referer': 'http://www.lianmenhu.com/ecology/',
        'Upgrade - Insecure - Requests': '1',
        'Connection': 'keep-alive',
    }



    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")
        trlist = response.xpath("//div[@class='m-news']/ul/li")
        print('trlist',trlist)
        for tr in trlist:
            item = LianmenhuItem()
            item['title'] = tr.xpath(".//div[@class='m-news-text rt']/h2/a/text()").extract_first()
          #  item['imgurl'] = tr.xpath(".//div[@class='m-news-pic lf']//img/@src").extract_first()
            item['summary'] = tr.xpath(".//div[@class='m-news-text rt']/p/text()").extract_first().strip()
            item['url'] = tr.xpath(".//h2/a/@href").extract_first()
           # item['imgurl'] = response.urljoin(item['imgurl'])
            item['url'] = response.urljoin(item['url'])
            item['reporttime'] = tr.xpath(".//span[@class='m-news-timer']/text()").extract_first().strip()

            print('打印',item)
            yield scrapy.Request(item["url"],
                                 callback=self.parse_detail,
                                 meta={"item": item},
                                 )
            if '前' in item['reporttime']:
                item['reporttime'] = str_YMD
            else:
                item['reporttime'] = item['reporttime'][0:10]
          #  if item['reporttime'] >= str_YMD:
              #item["url"]
            #    yield scrapy.Request(item["url"],
            #     callback=self.parse_detail,
            #     meta={"item": item},
        next_page = response.xpath("//div[@class='pg']/a[@class='nxt']/@href").extract_first()

        if next_page is not None:

            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
    #)
    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]

      #  item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
        #item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first().replace("\n","").replace("\r","")

        zw = []
        content_img = []
        zw_content = response.xpath("//div[@class='artical-content'][1]")
        content = response.xpath("//div[@id='content'][1]/div")
        print(content)
        for cn in content:
           # item["content"] = cn.xpath("./p").extract_first()
            img = cn.xpath('.//p/img/@src')
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
        print('item', item)
        yield item
