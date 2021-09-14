import scrapy
from ai_industry_reports.items import AiIndustryReportsItem
import datetime
import time
import json

class AiIndustryReportSpider(scrapy.Spider):
    name = 'ai_industry_report'
    #now_time = datetime.datetime.now().strftime('%Y-%m-%d')
    #lastweek_time = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime('%Y-%m-%d')
    #print('打印日期', now_time, lastweek_time)
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'Host': 'reportapi.eastmoney.com',
        'Connection': 'keep-alive',
        'Referer': 'http://data.eastmoney.com/',
        'Cookie':'qgqp_b_id=57ac516dfb68975a38c2e76fdc12fc5d; st_si=87678634560490; st_sn=54; st_psi=20210524104958416-113300303759-4023652879; st_asi=delete; cowCookie=true; intellpositionL=1136px; intellpositionT=2242px; st_pvi=81858586948633; st_sp=2021-05-12%2014%3A45%3A04; st_inirUrl=http%3A%2F%2Fxinsanban.eastmoney.com%2FDataCenter%2FCompanyListing%2FListingDetails'
    }
    def start_requests(self): # 获取列表业的网址
        end_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        start_year = int(time.strftime('%Y', time.localtime(time.time()))) - 2
        month_day = time.strftime('%m-%d', time.localtime(time.time()))
        start_time = '{}-{}'.format(start_year, month_day)

        for orgCode in ['81058029', '80536646', '80623328', '81084604']:
            #'80967691','80635291','80917054', '80365131', '80922601','80082012', '80001777', '80025306', '80489204','81052047', '81046906', '80203768', '80965769', '80064304','80491731', '80491458', '80523318', '81057808','81058029', '80536646', '80623328', '81084604'
            for page_num in range(5):
                url= 'http://reportapi.eastmoney.com/report/dg?' + '&pageNo={}'.format(page_num) + '&pageSize=50&author=*&orgCode={}'.format(orgCode) + '&beginTime={}'.format(start_time) + '&endTime={}'.format(end_time)
                print(url)
                yield scrapy.Request(url=url, callback=self.parse,headers=self.headers,dont_filter=True )

    def parse(self, response): #解析json
        jsonlist = json.loads(response.text)
        data = jsonlist.get('data')
        for i in data:
            item = AiIndustryReportsItem()
            item["title"] = i['title']
            item["orgName"] = i['orgName']
            item["orgSName"] = i['orgSName']
            item["publishDate"] = i['publishDate']
            item["industryName"] = i['industryName']
            item["researcher"] = i['researcher']
            item["url"] = 'http://data.eastmoney.com/report/zw_industry.jshtml?infocode=' + str(i["infoCode"])
            item["source"] = i['orgName']
            # yield item
            yield scrapy.Request(item["url"], callback=self.parse_detail, meta={"item": item}, )

    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]

      #  item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
        #item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first().replace("\n","").replace("\r","")

        item["pdfurl"]=response.xpath("//div[@class='c-infos']/span[@class='to-link']/a/@href").extract_first()

        print('item', item)
        yield item
