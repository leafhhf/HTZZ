# -*- coding: utf-8 -*-
import scrapy
from minglvetech.items import MinglvetechItem

class MinglveSpider(scrapy.Spider):
    name = 'minglve'
    #allowed_domains = ['https://mip.mininglamp.com/']
    start_urls = ['https://www.mininglamp.com/gather/information/12?productPage=1&industryPage=1&solutionPage=1&keyword=&biao=information&informationtype=0&informationPage=1']
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Host': 'www.mininglamp.com',
        'Connection': 'keep-alive',
        # 'Cookie': 'Hm_lvt_51dc371c890968cb9b33cd09db75c5d4=1623825049; has_js=1; Hm_lpvt_51dc371c890968cb9b33cd09db75c5d4=1623899126; _ga=GA1.2.1427897602.1623834634; _gid=GA1.2.776529567.1623834634; uniqueVisitorId=cf6ef1be-3a1b-3184-e479-d596422ec500; acw_tc=2f624a4416238990923841194e75c5cff9dcdb43774a475deefc9eea1de1ef',
        #'Referer': 'https://mip.mininglamp.com',
        'Upgrade - Insecure - Requests': '1',
        'Connection': 'keep-alive',
        #'If-Modified-Since':'Fri, 25 Dec 2020 10:05:21 GMT',
        #'If-None-Match':'W/"4367745667-6844-2020-12-25T10:05:21.874Z"',
    }

    # def start_requests(self):
    #     for i in range(0, 11):
    #         url = 'https://www.yitutech.com/cn/news?page={}'.format(i)
    #         print(url)
    #         yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)


    def parse(self, response):
        print("URL: " + response.request.url)
        trlist = response.xpath("//ul[@class='update-list']/li")
        print('trlist',trlist)
        for tr in trlist:
            item = MinglvetechItem()
            item['title'] = tr.xpath(".//div[@class='text']/text()").extract_first()
            item['url'] = tr.xpath(".//@onclick").extract_first()

            # item['url'].replace("ck	location.href='",'')
            item['url'] =item['url'][item['url'].find("/informationdetail/"):-1]
           # item['imgurl'] = response.urljoin(item['imgurl'])
            item['url'] = response.urljoin(item['url'])
            item['report_time'] = tr.xpath(".//div[@class='time']/text()").extract_first().strip()
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
        content = response.xpath("//div[@class='detail-diy']/*")
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
