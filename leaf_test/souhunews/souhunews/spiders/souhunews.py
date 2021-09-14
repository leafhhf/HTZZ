import scrapy
from souhunews.items import SouhunewsItem
import json
import datetime

class SouhunewsSpider(scrapy.Spider):
    name = 'souhunews'
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'Host': 'v2.sohu.com',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sohu.com/tag/59953?spm=smpc.ch30.fd-ctag.28.1621305204383Aez9o3H',
        'Cookie': 't=1621305216488; SUV=1620874007014heevg7; gidinf=x099980109ee1353c463e045e00054d618802ec0addc; IPLOC=CN1100; reqtype=pc',
        'Connection': 'keep-alive',
    }

    def start_requests(self):
        for sceneid in ['59953',"60035"]:
            #format(sceneid=sceneid)
            # for i in range(5):
            #   url = 'https://v2.sohu.com/public-api/feed?scene=TAG&sceneId={}'.format(sceneid)+'&page={page}'.format(page=i)+'&size=20'
            for i in range(1,50):
                url = 'https://v2.sohu.com/public-api/feed?scene=TAG&sceneId={}'.format(sceneid)+'&size=20&page={page}'.format(page=i)
                print(url)
                yield scrapy.Request(url=url, callback=self.parse, headers=self.headers, dont_filter=True)



    def parse(self, response):
        #
        now_time = datetime.datetime.now()+datetime.timedelta(days=-1)
        str_YMD = now_time.strftime("%Y-%m-%d")
        jsonlist = json.loads(response.text)
       # data = jsonlist.get('array')
        for i in jsonlist:
            item = SouhunewsItem()
            item["id"] = i['id']
            item["authorId"] = i['authorId']
            item["authorName"] = i['authorName']
            item["picUrl"] = i['picUrl']
            item["images"] = i['images']
            item["title"] = i['title']
            item["url"] ='https://www.sohu.com/a/'+str(item["id"])+'_'+str(item["authorId"])+'?spm=smpc.tag-page.fd-news'

            yield scrapy.Request(item["url"],
                                 callback=self.parse_detail,
                                 meta={"item": item},
                                 )

    def parse_detail(self, response):  # 处理详情页
        item = response.meta["item"]

      #  item["title"] = response.xpath("//div[@class='title']/h1/text()").extract_first()
        #item["source"] = response.xpath("//div[@class='artical-relative clearfix']/a/@title").extract_first().replace("\n","").replace("\r","")

        item["reporttime"]=response.xpath("//div[@class ='article-info']/span[@class ='time']/text()").extract_first()
        zw = []
        content_img = []
        content = response.xpath("//article[@class='article']/p")
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

        item["content_img"] = content_img

        item["content"] ="\n" .join(item["content"])
        item["content_img"] = ",".join(item["content_img"])

       # print('item', item)
        yield item
