# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from scrapy.spiders import Spider
from bvd.items import BvdItem



class OrbisSpider(scrapy.Spider):
    name = 'orbis'

    headers = {
        'Cookie': 'group_47782=Beijing university-35738; LANGUAGE=cn; _hp2_id.1984150978=%7B%22userId%22%3A%223829978031738980%22%2C%22pageviewId%22%3A%222267587372883778%22%2C%22sessionId%22%3A%221007054837747394%22%2C%22identity%22%3A%2216502696%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D; BVDCookieSecure=de1d401dc3eaa28c57d2c5600051; ctxorbis=2Y1MJDWCFXY75DR/6BD78IEL; _hp2_ses_props.1984150978=%7B%22ts%22%3A1624495942668%2C%22d%22%3A%22orbis.bvdinfo.com%22%2C%22h%22%3A%22%2Fversion-202164%2Forbis%2F1%2FCompanies%2FSearch%22%7D; __RequestVerificationToken=S_v6mknGz1SaJFxDxm9ABu0wZeg1SGY4FHVsuh8TvoRSBd7qGHPfQUKONqBVueuW7qyByzaWkiSbsILFUKW8EGVBPjo1',
        'Referer': 'https://orbis.bvdinfo.com/version-202164/orbis/1/Companies/List',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'# 设置请求头文件，模拟浏览器访问（涉及反爬机制）

    }

    def start_requests(self):  # 该函数定义需要爬取的初始链接并下载网页内容

        url = 'https://orbis.bvdinfo.com/version-202164/orbis/1/Companies/List'
        yield Request(url, headers=self.headers)

    def parse(self, response):
        item = BvdItem()
        left_table = response.xpath("//div[@class='fixed-data']/table/tbody")
        for data in left_table:
            item['company_name'] = data.xpath(".//td[@class='columnAlignLeft']/span/a/text()").extract()[0]
            print(item)
            yield item
