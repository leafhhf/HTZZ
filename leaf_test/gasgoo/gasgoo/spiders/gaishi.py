# -*- coding: utf-8 -*-
import scrapy
from gasgoo.items import GasgooItem
import datetime
import time
import json

class GaishiSpider(scrapy.Spider):
    name = 'gaishi'
    #now_time = datetime.datetime.now().strftime('%Y-%m-%d')
    #lastweek_time = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime('%Y-%m-%d')
    #print('打印日期', now_time, lastweek_time)

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'Host': 'i.gasgoo.com',
        'Connection': 'keep-alive',
        'Referer': ' http://i.gasgoo.com/supplier/c-0.html',
        # 'Cookie':'qgqp_b_id=57ac516dfb68975a38c2e76fdc12fc5d; st_si=87678634560490; st_sn=54; st_psi=20210524104958416-113300303759-4023652879; st_asi=delete; cowCookie=true; intellpositionL=1136px; intellpositionT=2242px; st_pvi=81858586948633; st_sp=2021-05-12%2014%3A45%3A04; st_inirUrl=http%3A%2F%2Fxinsanban.eastmoney.com%2FDataCenter%2FCompanyListing%2FListingDetails'
    }
    def start_requests(self): # 获取列表业的网址
        for page_num in range(0,1000):
            if page_num ==0:
                url = 'http://i.gasgoo.com/supplier/c-0.html'
            else:
                url= 'http://i.gasgoo.com/supplier/c-0/index-{}.html'.format(page_num)
        #     url = 'http://i.gasgoo.com/supplier/c-0/index-9.html'
            print(url)
            yield scrapy.Request(url=url, callback=self.parse,headers=self.headers,dont_filter=True )

    def parse(self, response):
        print("URL: " + response.request.url)
        trlist = response.xpath("//div[@class='Tabledemo']")
        for tr in trlist:
            item = GasgooItem()
            item['current_url'] = response.request.url
            item['com'] = tr.xpath(".//a[@target = '_blank' ]/text()").extract_first()
            item['url'] = tr.xpath(".//table[@class = 'neiwid']/tbody/tr/td/p/a[@target = '_blank' ]/@href").extract_first()
            item['url'] = item['url'] + "/"
            print('打印', item)

            yield scrapy.Request(item["url"],meta={'item': item},callback=self.parse_detail )

    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]
        item['com_jianjie'] = response.xpath("//div[@class = 'comText']/p/text() | //div[@class= 'newcomInfo']/p/text()").extract()
        item['totol_employee'] = response.xpath("//td[contains(text(),'人员规模')]/following-sibling::*[1]/text()").extract_first()

        item['research_employee'] = response.xpath("//td[contains(text(),'研发人数')]/following-sibling::*[1]/text()").extract_first()
        item['yearly_sales'] = response.xpath("//td[contains(text(),'年销售额')]/following-sibling::*[1]/text()").extract_first()
        item['direct_export_experience'] = response.xpath("//td[contains(text(),'直接出口经验')]/following-sibling::*/text()").extract_first()
        item['synchronous_development_capability'] = response.xpath("//td[contains(text(),'同步开发能力')]/following-sibling::*[1]/text()").extract_first()
        item['com_url'] = response.xpath("//td[contains(text(),'公司网址')]/following-sibling::*[1]/text()").extract_first()

        item['directly_supporting'] = response.xpath("//td[contains(text(),'直接配套')]/text()").extract_first()
        if item['directly_supporting']:
            item['directly_supporting'] =item['directly_supporting'].replace("直接配套：","")

        item['first_major_customer'] = response.xpath("//td[contains(text(),'第一主要客户')]/text()").extract_first()
        if item['first_major_customer']:
            item['first_major_customer'] = item['first_major_customer'].replace("第一主要客户：", "")
        item['first_major_customer_business_proportion'] = response.xpath("//td[contains(text(),'第一主要客户')]/following-sibling::*[1]/text()").extract_first()
        if item['first_major_customer_business_proportion']:
            item['first_major_customer_business_proportion'] =item['first_major_customer_business_proportion'].replace("业务占比：", "")

        item['second_major_customer'] = response.xpath("//td[contains(text(),'第二主要客户')]/text()").extract_first()
        if item['second_major_customer']:
            item['second_major_customer'] = item['second_major_customer'].replace("第二主要客户：", "")
        item['second_major_customer_business_proportion'] = response.xpath("//td[contains(text(),'第二主要客户')]/following-sibling::*[1]/text()").extract_first()

        if item['second_major_customer_business_proportion']:
            item['second_major_customer_business_proportion']=item['second_major_customer_business_proportion'].replace("业务占比：", "")

        item['third_major_customer'] = response.xpath("//td[contains(text(),'第三主要客户')]/text()").extract_first()
        if item['third_major_customer']:
            item['third_major_customer'] = item['third_major_customer'].replace("第三主要客户：", "")
        item['third_major_customer_business_proportion'] = response.xpath("//td[contains(text(),'第三主要客户')]/following-sibling::*[1]/text()").extract_first()
        if item['third_major_customer_business_proportion']:
            item['third_major_customer_business_proportion']=item['third_major_customer_business_proportion'].replace("业务占比：", "")

        item['indirect_supporting'] = response.xpath("//td[contains(text(),'间接配套')]/text()").extract_first()
        if item['indirect_supporting']:
            item['indirect_supporting'] = item['indirect_supporting'].replace("间接配套：", "")

        item['reg_capital'] = response.xpath("//td[contains(text(),'注册资本')]/following-sibling::*[1]/text()").extract_first()
        item['export_market'] = response.xpath("//td[contains(text(),'出口市场')]/following-sibling::*[1]/text()").extract_first()
        item['primary_product'] = response.xpath("//td[contains(text(),'主营产品')]/following-sibling::*[1]/text()").extract_first()
        item['taxpayer_registration_number'] = response.xpath("//td[contains(text(),'纳税人识别号')]/following-sibling::*[1]/text()").extract_first()
        print('item', item)
        yield item




