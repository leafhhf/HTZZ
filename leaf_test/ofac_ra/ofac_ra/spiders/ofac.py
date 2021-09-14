# -*- coding: utf-8 -*-
import scrapy
from ofac_ra.items import OfacRaItem
#from ai_news.settings import JUZI_PWD, JUZI_USER
import json
import datetime
import time
from scrapy.spiders import CrawlSpider, Rule
import hashlib
import requests
import os
from urllib import response

class OfacSpider(scrapy.Spider):
    name = 'ofac'
    domain = "https://home.treasury.gov"
    start_urls = ['https://home.treasury.gov/policy-issues/financial-sanctions/recent-actions?qry=China&ra-start-date=&ra-end-date=&page=0']

    headers = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        #Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0
        ## 这里必须有，才会有下面的解析方式
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://home.treasury.gov/policy-issues/financial-sanctions/recent-actions',
        'Cookie' :'_ga=GA1.2.1002361293.1623377937; _ga=GA1.3.1002361293.1623377937; _gid=GA1.2.1666622978.1624430204; _gid=GA1.3.1666622978.1624430204; _gat=1; _gat_GSA_ENOR0=1; _gat_GSA_ENOR1=1',
        'Connection': 'keep-alive',

    }

    # def start_requests(self):
    #     for i in range(1,50):
    #         url = 'https://www.ofweek.com/ai/CATList-201700-{page}-30436968,30436767,30436352,30436166,30435983,30435786,30435530.htm'.format(page=i)
    #         print(url)
    #         yield scrapy.Request(url=url, callback=self.parse,headers=self.headers)


    def parse(self, response):
        trlist = response.xpath("//div[@class='views-element-container']/div/div[@class='view-content']/div")
        #p1：//div[@class='view view-ofac-recent-actions view-id-ofac_recent_actions view-display-id-page_1 js-view-dom-id-6cd01f790075b4a0ab8f167598090165e0aa238533a079492eab05d74c0906b9']/div[@class='view-content']/div
        #p2://div[@class='view view-ofac-recent-actions view-id-ofac_recent_actions view-display-id-page_1 js-view-dom-id-33cbb4a8c7c692787fa066aa516c0d9b3890ac24db6e666c4b4c976ac1d76d2e']/div[@class='view-content']/div
        #p3:trlist = response.xpath("//div[@class='view view-ofac-recent-actions view-id-ofac_recent_actions view-display-id-page_1 js-view-dom-id-f7a6aa9ef513642708138f456c60847e786ab8c9d0f81ece0430b64abd2c3432']/div[@class='view-content']/div")
        #p4:trlist = response.xpath("//div[@class='view view-ofac-recent-actions view-id-ofac_recent_actions view-display-id-page_1 js-view-dom-id-b47c33028413bcaf4bc7bd5cd00fbc3bfa20a216e1a3ec4387fcdde68283a52a']/div[@class='view-content']/div")
        print('trlist',trlist)
        for tr in trlist:
            item = OfacRaItem()
            #item['title'] = tr.xpath(".//h3[@class='featured-stories__headline']/a/text()").extract_first()
            # item['title'] = tr.xpath(".//h2[@class='featured-stories__headline']/span/a/text()").extract_first()
            temp = tr.xpath(".//h2[@class='featured-stories__headline']/span/a/text()").extract_first()
            item['title'] = temp.replace("'", "‘")
            item['url'] = tr.xpath(".//h2[@class='featured-stories__headline']/span/a/@href").extract_first()
            item['url'] = response.urljoin(item['url'])
            item['report_time'] = tr.xpath(".//span[@class='date-format']/div/time/text()").extract_first().strip()
            item['report_time']=item['report_time'].replace(',','')
            item['report_time'] = datetime.datetime.strptime(item['report_time'], '%B %d %Y')
            item['report_time'] = item['report_time'].strftime("%Y-%m-%d")
            print('打印',item)
            yield scrapy.Request(item["url"],
                                 callback=self.parse_detail,
                                 meta={"item": item},
                                 )



    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]
        zw = []
        content = response.xpath(".//div[@class='field__item']/p")
        #content2 =response.xpath(".//div[@class='field field--name-field-news-body field--type-text-long field--label-hidden field__item']/div")
        #content = content1 +content2
        #print(content)
        for cn in content:
            text = cn.xpath('.//text()').extract()
            text = "".join(text)
            #text.replace("'", "’")
            if text:
                zw.append(text.strip('\n'))
                #  zw.append(text)
        item["content"] = zw
        print('content',item["content"])
        item["content"] ="\n" .join(item["content"])
        print('item', item)
        yield item

