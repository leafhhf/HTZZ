# -*- coding: utf-8 -*-
import scrapy
from chinagov.items import ChinagovItem
import datetime
import  time
from selenium import webdriver

from selenium.webdriver import FirefoxOptions

class ChngovSpider(scrapy.Spider):
    name = 'chngov'
    # start_urls = ['http://sousuo.gov.cn/column/30469/0.htm']
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
        'Host': 'sousuo.gov.cn',
        'Connection': 'keep-alive',
        # 'Cookie': 'vk=3939688b-1d11-451c-99a5-b6f347ac66ad; deviceid=QCxK8pLr; HWWAFSESID=2e6be730242d3997f3; HWWAFSESTIME=1624946667061; locale=zh-cn; SessionID=62b339f4-c833-4ad5-bb4b-53d940ba3b1d; ad_sc=; ad_mdm=; ad_cmp=; ad_ctt=; ad_tm=; ad_adp=; cf=Direct',
        'Referer': 'http://sousuo.gov.cn/column/30469/0.htm',
        # 'Upgrade- Insecure - Requests': '1',
        #'If-Modified-Since':'Fri, 25 Dec 2020 10:05:21 GMT',
        #'If-None-Match':'W/"4367745667-6844-2020-12-25T10:05:21.874Z"',
    }



    def start_requests(self):
        for i in range(0, 70):
            url = 'http://sousuo.gov.cn/column/30469/{page}.htm'.format(page=i)
            print(url)
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)


    def parse(self, response):
        print("URL: " + response.request.url)
        trlist = response.xpath("//ul[@class='listTxt']/li")
        # print('trlist',trlist)
        for tr in trlist:
            item = ChinagovItem()
            item['title'] = tr.xpath(".//h4/a/text()").extract_first()
            item['url'] = tr.xpath(".//h4/a/@href").extract_first()
            # item['url'] = item['url'].replace("//","")
           # item['imgurl'] = response.urljoin(item['imgurl'])
           #  item['url'] = "http://"+ item['url']
           #  item['public_date'] = response.xpath(".//h4/span/text()").extract_first()
           #  item['report_time'] = tr.xpath(".//p[@class='time']/text()").extract_first().strip()
            # item['report_time'] = time.strptime(item['report_time'], "%Y年%m月%d日")
            # item['report_time'] = time.strftime("%Y-%m-%d", item['report_time'])
            # print('打印',item)
            yield scrapy.Request(item["url"],
                                 callback=self.parse_detail,
                                 meta={"item": item},
                                 )


    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]
        item['title_class'] = response.xpath("//b[contains(text(),'主题分类')]/../following-sibling::*/text()").extract_first()
        item['issuer'] = response.xpath("//b[contains(text(),'发文机关')]/../following-sibling::*[1]/text()").extract_first()
        item['policy_number'] = response.xpath("//b[contains(text(),'发文字号')]/../following-sibling::*[1]/text()").extract_first()
        item['public_date'] = response.xpath("// b[contains(text(), '发布日期')] /../ following-sibling::*[1]/text() | //div[@class = 'pages-date']/text()").extract_first()
        length= len(item['public_date'])

        if length ==11 :
            item['public_date'] = time.strptime(item['public_date'], "%Y年%m月%d日")
            item['public_date'] = time.strftime("%Y-%m-%d %H:%M:%S", item['public_date'])
        elif length == 18:
            item['public_date'] = time.strptime(item['public_date'], "%Y-%m-%d %H:%M  ")
            item['public_date'] = time.strftime("%Y-%m-%d %H:%M:%S", item['public_date'])
        zw = []
        content_img = []
        #zw_content = response.xpath("//div[@class='artical-content'][1]")
        content = response.xpath("//td[@class = 'b12c']/p | //div[@id = 'UCAP-CONTENT']/p")
        # print(content)
        for cn in content:
           # item["content"] = cn.xpath("./p").extract_first()
           #  img = cn.xpath('.//img/@src')
            text = cn.xpath('.//text()').extract()
            text = "".join(text)
            if text:
                zw.append(text.strip('\n'))
                #  zw.append(text)
        item["text"] = zw
        print('text',item["text"])
        item["text"] ="\n" .join(item["text"])
        #item["content_img"] = ",".join(item["content_img"])
        print('item', item)
        yield item
