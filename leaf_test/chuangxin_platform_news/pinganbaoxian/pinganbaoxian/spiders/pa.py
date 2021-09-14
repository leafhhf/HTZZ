# -*- coding: utf-8 -*-
import scrapy
from pinganbaoxian.items import PinganbaoxianItem
import datetime
import  time
import requests


class PaSpider(scrapy.Spider):
    name = 'pa'
    # allowed_domains = ['http://www.ocft.com/news']
    # start_urls = ['http://www.ocft.com/news']


    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        "Cache-Control":'max-age=0',
        'Host': 'www.ocft.com',
        'Connection': 'keep-alive',
        # 'Cookie': 'JSESSIONID=FAF55AD71CEB1C269253FFF4F8898722',
        # 'Referer': 'http://www.ocft.com/news/type-newsReport-1',
        'Upgrade-Insecure-Requests': '1',
        'Connection': 'keep-alive',
        #'If-Modified-Since':'Fri, 25 Dec 2020 10:05:21 GMT',
        #'If-None-Match':'W/"4367745667-6844-2020-12-25T10:05:21.874Z"',
    }

    def start_requests(self):

        url_list = []
        for i in range(000,400):
            url_list.append('http://www.ocft.com/news/detail-{page}'.format(page=i))

        url_all = []
        for ul in url_list:
            st = requests.get(ul).status_code
            print(st)
            if st == 200:
                url_all.append(ul)
        print("url_all", url_all)

        for ul1 in url_all:
            yield scrapy.Request(url=ul1, callback=self.parse_detail, headers=self.headers)

        # url = 'http://www.ocft.com/news/detail-335'
        # st = requests.get(url).status_code
        # print(st)
        # if st == 200:
        #     yield scrapy.Request(url=url, callback=self.parse_detail, headers=self.headers)

    # def parse(self, response):
    #     print("URL: " + response.request.url)
    #     trlist = response.xpath("//ul[@id='news-list']/li")
    #     print('trlist',trlist)
    #     for tr in trlist:
    #         item = PinganbaoxianItem()
    #         # item['title'] = tr.xpath(".//h3/a/text()").extract_first().strip()
    #         item['url'] = tr.xpath(".//a/@href").extract_first()
    #        # item['imgurl'] = response.urljoin(item['imgurl'])
    #         item['url'] = response.urljoin(item['url'])
    #         item['report_time'] = tr.xpath(".//p[@class='item_n-date fs-mini']/text()").extract_first().strip()
    #         item['report_time'] = time.strptime(item['report_time'], "%Y-%m-%d %H:%M:%S")
    #         item['report_time'] = time.strftime("%Y-%m-%d", item['report_time'])
    #         print('打印',item)
    #         yield scrapy.Request(item["url"],
    #                              callback=self.parse_detail,
    #                              meta={"item": item},
    #                              )


    def parse_detail(self, response):
        item = PinganbaoxianItem()# 处理详情页
        # item = response.meta["item"]
        item['url'] = response.url
        zw = []
        content_img = []
        item['title'] = response.xpath("//div[@class='title-wap']/h1/text()").extract_first()
        a = item['title']. find('测试')
        if item['title'] and not a  :
            item['title'] = item['title'].strip()
        time1=response.xpath("//div[@class='time']/div/span[1]/text()").extract_first()
        time2=response.xpath("//div[@class='time']/div/span[2]/text()").extract_first()
        if time1 !='' and time2 != '':
            item['report_time']=time1+"/"+time2
            item['report_time'] = time.strptime(item['report_time'], "%Y/%m/%d")
            item['report_time'] = time.strftime("%Y-%m-%d", item['report_time'])
        #zw_content = response.xpath("//div[@class='artical-content'][1]")
        content = response.xpath("//div[@class='container fade-in-top']/p")
        print(content)
        if content:
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
        if len(content_img)==0:
            item['pic1_url']=""
            item['pic2_url'] = ""
            item['pic3_url'] = ""
        elif len(content_img) ==1:
            item['pic1_url'] = content_img[0]
            item['pic2_url']=""
            item['pic3_url'] =""
        elif len(content_img) ==2:
            item['pic1_url'] = content_img[0]
            item['pic2_url'] = content_img[1]
            item['pic3_url'] = ""
        else:
            item['pic1_url'] = content_img[0]
            item['pic2_url'] = content_img[1]
            item['pic3_url'] = content_img[2]
        # item["content_img"] = content_img

        print('content',item["content"])
        item["content"] ="\n" .join(item["content"])
        #item["content_img"] = ",".join(item["content_img"])

        print('item', item)
        yield item
