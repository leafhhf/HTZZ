# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from souhunews.items import SouhunewsItem
import json
import datetime
import time


class Souhu_spider(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "sohunews"

    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        'Host': 'v2.sohu.com',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sohu.com/tag/59953?spm=smpc.ch30.fd-ctag.28.1591263650172fosdoW2',
        'Cookie':'SUV=1906211542018915; gidinf=x099980109ee0fde2e565a01a000707a58743c8b6774; YYID=9D22D5DBBEA68492C2A88290C7C3D593; _muid_=1569579293388583; t=1591327109446; IPLOC=CN1100; debug_test=sohu_third_cookie; BAIDU_SSP_lcr=https://www.so.com/link?m=aDstb7a3Zx%2BWIgHLJI35%2B1Sf1TG1EhDkw38NJPTre3h9s8gWO2LnKJi5E8fKYfKEG2OYC9xUSWlIq0FAapq67t%2F3Av0c9ZZrzMfQlZGwYrwGr%2FSmomQ%2BdIW8XkpfGellbaPDyB%2BT2s3sU6E2K; reqtype=pc; MTV_SRC=11050001',
        'Connection': 'keep-alive',
    }

    def start_requests(self):
        for sceneid in ['48065','59953','48173','48174']:
            #format(sceneid=sceneid)
            for i in range(1,3):

                url = 'https://v2.sohu.com/public-api/feed?scene=TAG&sceneId={}'.format(sceneid)+'&size=20&page={page}'.format(page=i)
                print(url)
                yield scrapy.Request(url=url, callback=self.parse,headers=self.headers,dont_filter=True )
    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")
        jsonlist = json.loads(response.text)
       # data = jsonlist.get('array')
        for i in jsonlist:
            item = SouhunewsItem()
            item["id"] = i['id']
            item["authorId"] = i['authorId']
            item["authorName"] = i['authorName']
            item["picUrl"] = i['picUrl']
            item["images"] = i['images']
            item["title"] = i['title']
            item["url"] ='https://www.sohu.com/a/'+str(item["id"])+'_'+str(item["authorId"])+'?spm=smpc.tag-page.fd-news'

            yield scrapy.Request(item["url"],
                                 callback=self.parse_detail,
                                 meta={"item": item},
                                 )

    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]

      #  item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
        #item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first().replace("\n","").replace("\r","")

        item["reporttime"]=response.xpath("//div[@class ='article-info']/span[@class ='time']/text()").extract_first()
        zw = []
        content_img = []
        content = response.xpath("//article[@class='article']/p")
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

        item["content"] ="\n" .join(item["content"])
        item["content_img"] = ",".join(item["content_img"])

       # print('item', item)
        yield item
