# -*- coding:utf-8 -*-
from urllib import response

import scrapy
from ai_industry_reports.items import AiIndustryReportsItem
import json
import datetime
import time


class Industry_report_spider(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = "industry_report"
    now_time = datetime.datetime.now().strftime('%Y-%m-%d')
    lastweek_time = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime('%Y-%m-%d')
    print('打印日期',now_time,lastweek_time)
    #start_urls = ['https://reportapi.eastmoney.com/report/dg?pageNo=1&pageSize=50&author=*&orgCode=80917054&&beginTime={beginTime}'.format(beginTime=lastweek_time)+'&endTime={endTime}&fields=&qType='.format(endTime=now_time)]

    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
        'Host': 'reportapi.eastmoney.com',
        'Connection': 'keep-alive',
        'Referer': 'http://data.eastmoney.com/report/industry.jshtml',
        'Cookie':'qgqp_b_id=08dc30c03ad09447abb5e1ebc589dde2; HAList=a-sh-688555-%u6CFD%u8FBE%u6613%u76DB%2Ca-sz-000063-%u4E2D%u5174%u901A%u8BAF%2Ca-sh-603217-%u5143%u5229%u79D1%u6280%2Ca-sz-002761-%u591A%u559C%u7231%2Ca-sh-603109-%u795E%u9A70%u673A%u7535%2Cd-hk-00005%2Ca-sz-300111-%u5411%u65E5%u8475%2Ca-sz-300340-%u79D1%u6052%u80A1%u4EFD; em_hq_fls=js; _qddaz=QD.dop3f9.qjb2gu.jznf4lz4; pgv_pvi=1225191424; intellpositionL=1708.4px; intellpositionT=2471px; emshistory=%5B%22601727%22%2C%22601186%22%2C%22%E7%A7%91%E5%A4%A7%E8%AE%AF%E9%A3%9E%22%5D; cowCookie=true; st_si=56284791992226; st_sn=238; st_psi=20200608094256991-113300303753-1724951842; st_asi=delete; st_pvi=71670007251936; st_sp=2019-08-12%2008%3A51%3A18; st_inirUrl=http%3A%2F%2Fwww.so.com%2Flink',
    }
    def start_requests(self):
        now_time = datetime.datetime.now().strftime('%Y-%m-%d')
        lastweek_time = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
        for orgCode in ['81084604','80961165','80635291','80917054','80365131','80922601','80082012','80001777','80025306','80489204','81052047','81046906','80203768','80965769','80064304','80491731','80491458','80523318','81057808','10002352','81058029','80206788','80536646','81052522','81052048','80623328']:
        #  for orgCode in ['80961165','80635291','80917054']:
            url='https://reportapi.eastmoney.com/report/dg?pageNo=1&pageSize=50&author=*&orgCode={}'.format(orgCode)+'&fields=&qType=&beginTime={}'.format(lastweek_time)+'&endTime={}'.format(now_time)
            print(url)
            yield scrapy.Request(url=url, callback=self.parse,headers=self.headers,dont_filter=True )


    def parse(self, response):
        jsonlist = json.loads(response.text)
        data = jsonlist.get('data')
        for i in data:
            item = AiIndustryReportsItem()
            item["title"] = i['title']
            item["orgName"] = i['orgName']
            item["orgSName"] = i['orgSName']
            item["publishDate"] = i['publishDate'][0:10]
            item["industryName"] = i['industryName']
            item["researcher"] = i['researcher']
            item["url"] ='http://data.eastmoney.com/report/zw_industry.jshtml?encodeUrl='+str(i["encodeUrl"])
           # yield item

            yield scrapy.Request(item["url"], callback=self.parse_detail, meta={"item": item}, )

    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]

      #  item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
        #item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first().replace("\n","").replace("\r","")

        item["pdfurl"]=response.xpath("//div[@class='c-infos']/span[@class='to-link']/a/@href").extract_first()

        print('item', item)
        yield item
