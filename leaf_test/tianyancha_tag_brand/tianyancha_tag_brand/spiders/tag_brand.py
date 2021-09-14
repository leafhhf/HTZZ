import scrapy
from tianyancha_tag_brand.items import TianyanchaTagBrandItem
from scrapy.spiders import CrawlSpider, Rule
import requests
import os




class TagBrandSpider(scrapy.Spider):
    # 一定要一个全局唯一的爬虫名称，命令行启动的时候需要指定该名称
    name = 'tag_brand'
    allowed_domains = ["www.tianyancha.com"]
    # start_urls = ["https://top.tianyancha.com/brand-tag/b5b65500011762/p0"]
    headers={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Host":"www.top.tianyancha.com",
    'Cookie': 'TYCID=65e54330b2c411eb8ff121d950a5dba9; ssuid=3620383228; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2215901489692%22%2C%22first_id%22%3A%221795e4478fba-049b07d18c0d9f-4c3f2c72-2073600-1795e4478fc7bb%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%221795e4478fba-049b07d18…a0d3400e651ea02d7c903877bf6272618c8f50ecb; csrfToken=fDeyRWPLHWAd9SzDIwPjx_Vo; Hm_lpvt_ded2b36577e77da320c91b90209bf7ac=1624352379; tyc-user-info={%22state%22:%220%22%2C%22vipManager%22:%220%22%2C%22mobile%22:%2215901489692%22}; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTkwMTQ4OTY5MiIsImlhdCI6MTYyNDI2NzYxOSwiZXhwIjoxNjU1ODAzNjE5fQ.hgBPIwILNwtgjweoOGHVBnrH-cYgpjmyR590u__liS2MX00hDr2kSqFh9sMKyvL2MkIROe5f620GBrBze11pmQ; CT_TYCID=298dd38e625a43dab87646d2d5456175; cloud_token=234183b345dc4353a5a89d99ef55670f',
    "Referer":"https://top.tianyancha.com",

    }

    def start_requests(self):
        url = []
        for i in range(1,5):
            url = 'https://top.tianyancha.com/brand-tag/b5b65500011762/p{page}'.format(page=i)
            print('url', url)
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        trlist = response.xpath("//div[@class = 'search-item sv-search-company']")
        print('trlist',trlist)
        for tr in trlist:
            item = TianyanchaTagBrandItem()
            item['brand_name'] = tr.xpath(".//div[@class='header']/a/text()")
            item['brand_biaoqian_all']=tr.xpath(".//div[@class = 'tag-list'] /a/text()")
            item['brand_url']=tr.xpath('//div[@class="header"]/a/@href')
            item['company_name']=tr.xpath('//div[@class="title -wider text-ellipsis"]/a/text()')
            item['company_url']=tr.xpath('//div[@class="title -wider text-ellipsis"]/a/@href')
            item['company_founddate ']= tr.xpath("//div[@class='title  text-ellipsis']/span/@title |//div[@class='info row text-ellipsis']/div[@class='']/span/text()")
            item['company_area']= tr.xpath("//div[contains(@class,'title  text-ellipsis') and contains(text(),'所属地')]/span/text()")
            print('打印',item)
            yield item

        # next_page = response.xpath("//div[@class='pg']/a[@class='nxt']/@href").extract_first()
        #
        # if next_page is not None:
        #
        #     next_page = response.urljoin(next_page)
        #     yield scrapy.Request(next_page, callback=self.parse)
    #)
    # def parse_detail(self, response):  # 处理详情页
    #     item = response.meta["item"]
    #
    #   #  item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
    #     #item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first().replace("\n","").replace("\r","")
    #
    #     zw = []
    #     content_img = []
    #     zw_content = response.xpath("//div[@class='artical-content'][1]")
    #     content = response.xpath("//div[@id='content'][1]/div")
    #     print(content)
    #     for cn in content:
    #        # item["content"] = cn.xpath("./p").extract_first()
    #         img = cn.xpath('.//p/img/@src')
    #         text = cn.xpath('.//text()').extract()
    #         text = "".join(text)
    #         if img:
    #             zw.append('#img#')
    #             imgurl = response.urljoin(img.extract_first())
    #             #content_img.append(imgurl)
    #             content_img.append(imgurl)
    #         #if text and text.strip() != '\n':
    #         if text:
    #             zw.append(text.strip('\n'))
    #             #  zw.append(text)
    #     item["content"] = zw
    #
    #     item["content_img"] = content_img
    #
    #     print('content',item["content"])
    #     item["content"] ="\n" .join(item["content"])
    #     item["content_img"] = ",".join(item["content_img"])
    #     print('item', item)
    #     yield item
    #

