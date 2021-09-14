# -*- coding:utf-8 -*-
import scrapy
from treasurynews.items import TreasurynewsItem
#from ai_news.settings import JUZI_PWD, JUZI_USER
import json
import datetime
import time
from urllib import response

class NewsSpider(scrapy.Spider):
    name = 'news'
    domain = "https://home.treasury.gov"

    start_urls = ['https://home.treasury.gov/news/press-releases?title=China&publication-start-date=&publication-end-date=&page=1']
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Host': 'www.lianmenhu.com',
        'Connection': 'keep-alive',
        'Cookie': '_ga=GA1.2.1002361293.1623377937; _ga=GA1.3.1002361293.1623377937; _gid=GA1.2.2144732166.1623736524; _gid=GA1.3.2144732166.1623736524; _gat=1; _gat_GSA_ENOR0=1; _gat_GSA_ENOR1=1',
       # 'Referer': 'http://www.lianmenhu.com/apply/',
        'Referer': 'https://home.treasury.gov/news/press-releases?title=China&publication-start-date=&publication-end-date=&page=1',
        'Upgrade - Insecure - Requests': '1',
        'Connection': 'Trailers',
        'TE': 'keep-alive',
    }



    def parse(self, response):
        #
        #now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        #str_YMD = now_time.strftime("%Y-%m-%d")
        trlist = response.xpath("//div[@class='content--2col__body']/div")
        print('trlist',trlist)

        for tr in trlist:
            item = TreasurynewsItem()
            #item['title'] = tr.xpath(".//h3[@class='featured-stories__headline']/a/text()").extract_first()
            temp = tr.xpath(".//h3[@class='featured-stories__headline']/a/text()").extract_first()

            item['title'] = temp.replace("'", "‘")

            item['url'] = tr.xpath(".//h3[@class='featured-stories__headline']/a/@href").extract_first()
            item['url'] = response.urljoin(item['url'])
            item['report_time'] = tr.xpath(".//span[@class='date-format']/time/text()").extract_first().strip()
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
        content1 = response.xpath(".//div[@class='field field--name-field-news-body field--type-text-long field--label-hidden field__item']/p")
        content2 =response.xpath(".//div[@class='field field--name-field-news-body field--type-text-long field--label-hidden field__item']/div")
        content = content1 +content2
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
