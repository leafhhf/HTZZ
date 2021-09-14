# -*- coding: utf-8 -*-
import scrapy
from cpc_org_dep.items import CpcOrgDepItem
import datetime
import  time
from selenium import webdriver

from selenium.webdriver import FirefoxOptions

class OrgSpider(scrapy.Spider):
    name = 'org'
    # allowed_domains = ['https://www.12371.cn/special/zgwjk/']
    # start_urls = ['http://https://www.12371.cn/special/zgwjk//']
    browser = webdriver.Chrome()
    browser.get('https://www.12371.cn/special/zgwjk/')
    year_list = ["2021年","2020年","2019年","2018年","2017年","2016年","2015年","2014年","2013年","2012年"]
    title = []
    url_list = []
    crawl_year = []
    trlist_all = []
    year = "2021年"
    i = 0
    while i <= len(year_list):
        browser.find_element_by_xpath("//a[contains(text(),'{}')]".format(year_list[i])).click()
        trlist = browser.find_elements_by_xpath("//div[@id='one_upELMT1610591089528519']/li/div/a")
        for tr in trlist:
            trlist_all.append(tr)
            crawl_year.append(year_list[i])
        # 尝试翻页
        try:
            browser.find_element_by_xpath("//a[contains(text(),'下一页')]").click()
            trlist = browser.find_elements_by_xpath("//div[@id='one_upELMT1610591089528519']/li/div/a")
            for tr in trlist:
                trlist_all.append(tr)
                crawl_year.append(year)
        except Exception:
            print("未找到下一页")
            continue


        # trlist= browser.find_elements_by_xpath("//a[@class='dyw901_titleL']")
        for tr in trlist:
            url_list.append(tr.get_attribute('href'))
        for url in url_list:
            browser.get('{}'.format(url))
            title_list = browser.find_elements_by_xpath("//h1[@class='big_title']")
            title.append(title_list[0].text)


    url = browser.find_elements_by_xpath("//a[@class='dyw901_titleL']/@href")



    # def start_requests(self):
    #     for i in range(0, 70):
    #         url = 'http://sousuo.gov.cn/column/30469/{page}.htm'.format(page=i)
    #         print(url)
    #         yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        print("URL: " + response.request.url)
        trlist = response.xpath("//div[@id='one_upELMT1610591089528519']/li")
        # print('trlist',trlist)
        for tr in trlist:

            item['title'] = tr.xpath(".//a[@class='dyw901_titleL']/text()").extract_first()
            item['url'] = tr.xpath(".//a[@class='dyw901_titleL']/@href").extract_first()
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
