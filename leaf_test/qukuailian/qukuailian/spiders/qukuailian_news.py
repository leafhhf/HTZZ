import scrapy
from qukuailian.items import QukuailianItem
#from ai_news.settings import JUZI_PWD, JUZI_USER
import json
import datetime
import time


class QukuailianNewsSpider(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = 'qukuailian_news'
    domain = ['tech.china.com.cn/bc/']
    # 指定爬虫入口，scrapy支持多入口，所以一定是lis形式
    start_urls = ['http://tech.china.com.cn/bc/']
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Host': 'tech.china.com.cn',
        'Connection': 'keep-alive',
        'Cookie': 'wdcid=132cecef2561aa06; wdlast=1622770365; Hm_lvt_968892b3cc754de5583f6988acbd4190=1620874372,1622711717; Hm_lpvt_968892b3cc754de5583f6988acbd4190=1622770365',
        #'Referer': 'http://www.lianmenhu.com/ecology/',
        'Upgrade - Insecure - Requests': '1',
        'Connection': 'keep-alive',
    }



    def parse(self, response):
        #
        #now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        #str_YMD = now_time.strftime("%Y-%m-%d")
        trlist = response.xpath("//div[@class = 'htabx']/ul/li")
        print('trlist',trlist)
        for tr in trlist:
            item = QukuailianItem()
            item['title'] = tr.xpath(".//h3[@class='hsTit3']/a/text()").extract_first()
          #  item['imgurl'] = tr.xpath(".//div[@class='m-news-pic lf']//img/@src").extract_first()
            item['summary'] = tr.xpath(".//p[@class= 'lsTips']/text()").extract_first().strip()
            item['url'] = tr.xpath(".//h3[@class='hsTit3']/a/@href").extract_first()
           # item['imgurl'] = response.urljoin(item['imgurl'])
            item['url'] = response.urljoin(item['url'])
            item['reporttime'] = tr.xpath(".//div[@class = 'fl']/span/text()").extract_first().strip()
            print('打印',item)
            yield scrapy.Request(item["url"],
                                 callback=self.parse_detail,
                                 meta={"item": item},
                                 )
            # if '前' in item['reporttime']:
            #     item['reporttime'] = str_YMD
            # else:
            #     item['reporttime'] = item['reporttime'][0:10]
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
