# -*- coding: utf-8 -*-
import scrapy
from hikvision.items import HikvisionItem
import datetime
import requests
import time


class HikSpider(scrapy.Spider):
    name = 'hik'
    # allowed_domains = ['hik.com']
    # start_urls = ['https://www.hikvision.com/cn/NewsEvents/Newsroom//']
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Host': 'www.hikvision.com',
        'Connection': 'keep-alive',
        'Cookie': 'Hm_lvt_f77c9e9f600479d8302057e01f16f594=1625124262; Hm_lpvt_f77c9e9f600479d8302057e01f16f594=1625132859; AWSALB=3ggMG6L/d2zlidGr6BND27+OTQwDv6jNRQqOGNG5TcUIqrdpPo0xktKAsplBuc9K6WvChD0v6uL2YNF3R5cB1gCSzDX+QzgtiDDVgyzf/zN20TvZ9bzQzCtBDzaK; AWSALBCORS=3ggMG6L/d2zlidGr6BND27+OTQwDv6jNRQqOGNG5TcUIqrdpPo0xktKAsplBuc9K6WvChD0v6uL2YNF3R5cB1gCSzDX+QzgtiDDVgyzf/zN20TvZ9bzQzCtBDzaK; UM_distinctid=17a5acd71a63e5-08da046889d33a8-4c3f2d73-1fa400-17a5acd71a94cd; CNZZDATA1279943487=1511015607-1625018011-%7C1625131894',
        # 'Referer': 'https://www.hikvision.com/cn/NewsEvents/Newsroom/',
        'Upgrade - Insecure - Requests': '1',
        'Connection': 'keep-alive',
        'If-Modified-Since':'Wed, 30 Jun 2021 01:56:50 GMT',
        'If-None-Match':'W/"8b017-5c5f2092cb8ef-gzip"',
    }

    def start_requests(self):
        url_list = []
        starttime =datetime.datetime.now().strftime('%Y-%m-%d')
        date = datetime.datetime.strptime(starttime, '%Y-%m-%d')
        year = date.strftime('%Y')
        url0 = 'https://www.hikvision.com/content/hikvision/cn/NewsEvents/Newsroom/'
        url = url0 + year + '/' + starttime
        for j in range(0, 2):
            if j == 0:
                url_list.append(url + '/')
            else:
                url_list.append(url + '-' + str(j) + '/')

        # starttime = '2021-06-07'
        # date = datetime.datetime.strptime(starttime,'%Y-%m-%d')

        # url_list = []
        #
        # for i in range(1,2):
        #     date0 = date+datetime.timedelta(days=i)
        #     date1 = date0.strftime( '%Y-%m-%d')
        #     year = date0.strftime('%Y')
        #     url0 = 'https://www.hikvision.com/content/hikvision/cn/NewsEvents/Newsroom/'
        #     url = url0 + year + '/' + date1
        #     # print(url)
        #     for j in range(0,2):
        #         if j == 0:
        #             url_list.append(url+'/')
        #         else:
        #             url_list.append(url+'-'+ str(j)+'/')
        #
        # print("url_list",url_list)

        url_all = []
        for ul in url_list:
            st = requests.get(ul).status_code
            print(st)
            if st == 200:
                url_all.append(ul)

        print("url_all",url_all)

        for ul1 in url_all:
            yield scrapy.Request(url=ul1, callback=self.parse_detail, headers=self.headers)
            # else:
            #     for i in range(1,5):
            #         url = 'https://www.hikvision.com/content/hikvision/cn/NewsEvents/Newsroom/2021/2021-03-30-{}.html'.format(i)
            #         print(url)
            #         yield scrapy.Request(url=url, callback=self.parse_detail, headers=self.headers)

    #
    # def parse_url(self, response):
    #     news_url = response.xpath("//div[@class='mobile-nav-li']/p/a/@href").extract()
    #     url_final = 'https://www.hikvision.com' + str(news_url[8])
    #     print(url_final)
    #     yield scrapy.Request(url=url_final, callback=self.parse, headers=self.headers)


    # def parse(self, response):
    #     print("URL: " + response.request.url)
    #     # trlist = response.xpath("//dd[@class='solve-dd nav-dd']/li")
    #     trlist = response.xpath("//div[@class='news-event-list__layout1-wrapper pagination-container']/ul/li")
    #     print('trlist',trlist)
    #     for tr in trlist:
    #         item = HikvisionItem()
    #         item['title'] = tr.xpath(".//p[@class='tile-heading text-primary-color']/text()").extract_first()
    #         item['url'] = tr.xpath(".//a/@href").extract_first()
    #         item['url'] = response.urljoin(item['url'])
    #         # item['report_time'] = tr.xpath(".//li[@class='item']/span[@class='details']/text()").extract_first().strip()
    #         # item['report_time'] = time.strptime(item['report_time'], "%Y年%m月%d日")
    #         # item['report_time'] = time.strftime("%Y-%m-%d", item['report_time'])
    #         print('打印',item)
    #         yield scrapy.Request(item["url"],
    #                              callback=self.parse_detail,
    #                              meta={"item": item},
    #                              )


    def parse_detail(self, response):  # 处理详情页
        # item = response.meta["item"]
        item = HikvisionItem()
        print(response.url)
        zw = []
        content_img = []
        item['url'] = response.url
        item['report_time'] = item['url'][item['url'].find("2021-"):item['url'].find("2021-")+10]
        # item['report_time'] =time
        item['title'] = response.xpath("//div[@class='title title-2 title-uppercase text-primary-color-2']/text()").extract_first().strip()
        # item['url']=response.xpath("//body/@data-page-path")
        # item['url'] = item['url']
        # content = response.xpath("//div[@class='text aem-GridColumn--default--none aem-GridColumn--phone--none aem-GridColumn aem-GridColumn--phone--9 aem-GridColumn--offset--phone--1 aem-GridColumn--offset--default--5 aem-GridColumn--default--5']/div/p |//div[@class='text aem-GridColumn--default--none aem-GridColumn aem-GridColumn--default--8 aem-GridColumn--offset--default--2']/div[@class='cmp-text'] /* | //div[@class='image aem-GridColumn--default--none aem-GridColumn aem-GridColumn--default--8 aem-GridColumn--offset--default--2']/div/* ")
        content = response.xpath('//div[@class="text aem-GridColumn--default--none aem-GridColumn aem-GridColumn--default--8 aem-GridColumn--offset--default--2"]/div/p | //div[contains(@class,"image aem-GridColumn--default--none")]/div')
        print(content)
        for cn in content:
           # item["content"] = cn.xpath("./p").extract_first()
           #  item["report_time"] = cn.xpath("//div[@class='text aem-GridColumn--default--none aem-GridColumn--phone--none aem-GridColumn aem-GridColumn--phone--9 aem-GridColumn--offset--phone--1 aem-GridColumn--offset--default--5 aem-GridColumn--default--5']/div/p/text()")
            img = cn.xpath(".//@data-cmp-src")
            text = cn.xpath('.//text()').extract()
            text = "".join(text)
            if img:
                zw.append('#img#')
                imgurl = response.urljoin(img.extract_first())
                imgurl = imgurl.replace("{.width}","")
                imgurl = imgurl[:imgurl.find("100.")] + "100.1280." + imgurl[imgurl.find("jpeg"):]
                content_img.append(imgurl)

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
        item["content_img"] = content_img
        print('content',item["content"])
        item["content"] ="\n" .join(item["content"])
        #item["content_img"] = ",".join(item["content_img"])

        print('item', item)
        yield item
