# -*- coding: utf-8 -*-
import scrapy
from miying.items import MiyingItem
import datetime
import  time

class MySpider(scrapy.Spider):
    name = 'my'
    # allowed_domains = ['miying.com']
    # start_urls = ['https://miying.qq.com/official/mobile/news']

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Host': 'miying.qq.com',
        'Connection': 'keep-alive',
        # 'Cookie': 'vk=3939688b-1d11-451c-99a5-b6f347ac66ad; deviceid=QCxK8pLr; HWWAFSESID=2e6be730242d3997f3; HWWAFSESTIME=1624946667061; locale=zh-cn; SessionID=62b339f4-c833-4ad5-bb4b-53d940ba3b1d; ad_sc=; ad_mdm=; ad_cmp=; ad_ctt=; ad_tm=; ad_adp=; cf=Direct',
        # 'Referer': 'https://www.megvii.com/news/?cate=1&page=1',
        'Upgrade - Insecure - Requests': '1',
        'Connection': 'keep-alive',
        #'If-Modified-Since':'Fri, 25 Dec 2020 10:05:21 GMT',
        #'If-None-Match':'W/"4367745667-6844-2020-12-25T10:05:21.874Z"',
    }

    def start_requests(self):
        for i  in range(1,2):
            url = 'https://miying.qq.com/official/news/{page}'.format(page=i)
        # for i in range(1, 22):
        #     url = 'https://www.sensetime.com/cn/news-press-release?pagenum={page}&pagesize=9'.format(page=i)
            print(url)
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)


    def parse(self, response):
        print("URL: " + response.request.url)
        trlist = response.xpath("//ul[@id='content-list']/li")
        print('trlist',trlist)
        for tr in trlist:
            item = MiyingItem()
            item['title'] = tr.xpath(".//h3/a/text()").extract_first().strip()
            item['url'] = tr.xpath(".//h3/a/@href").extract_first()
           # item['imgurl'] = response.urljoin(item['imgurl'])
            item['url'] = response.urljoin(item['url'])
            item['report_time'] = tr.xpath(".//p[@class='hint']/span[1]/text()").extract_first().strip()
            item['report_time'] = time.strptime(item['report_time'], "%Y/%m/%d")
            item['report_time'] = time.strftime("%Y-%m-%d", item['report_time'])
            print('打印',item)
            yield scrapy.Request(item["url"],
                                 callback=self.parse_detail,
                                 meta={"item": item},
                                 )


    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]
        zw = []
        content_img = []
        #zw_content = response.xpath("//div[@class='artical-content'][1]")
        content = response.xpath("//div[@class='news-cont']/*")
        print(content)
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
