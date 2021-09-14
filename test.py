# -*- coding: utf-8 -*-

"""
起始未登录有反爬，爬失败。策略：重新登录，并在headers中加入cookie，成功了。
"""
import requests
from lxml import etree
import json
import time
import random
from copy import deepcopy
class Tianyan():
    def __init__(self):
        self.url = 'https://top.tianyancha.com/brand-tag/{}/p{}'
        self.headers={
        'Cookie': 'TYCID=65e54330b2c411eb8ff121d950a5dba9; ssuid=3620383228; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2215901489692%22%2C%22first_id%22%3A%221795e4478fba-049b07d18c0d9f-4c3f2c72-2073600-1795e4478fc7bb%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22%24device_id%22%3A%221795e4478fba-049b07d18c0d9f-4c3f2c72-2073600-1795e4478fc7bb%22%7D; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1623997643,1624001118,1624008433,1624240790; _ga=GA1.2.910947874.1620784218; tyc-user-info-save-time=1624267619327; tyc-user-phone=%255B%252215901489692%2522%255D; Hm_lvt_ded2b36577e77da320c91b90209bf7ac=1624001101,1624242338,1624266072,1624267621; jsid=https%3A%2F%2Fwww.tianyancha.com%2F%3Fjsid%3DSEM-BAIDU-CG-CX-000003%26bd_vid%3D7119219953419302336%26userid%3D31769301%26query%3D%25B2%25E9%25D1%25AF%25C9%25CF%25CA%25D0%25B9%25AB%25CB%25BE%26keywordid%3D245568718064%26campaignid%3D150848339%26groupid%3D5662561552; searchSessionId=1624242509.88492902; bannerFlag=true; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1624322685; _gid=GA1.2.1770986815.1624240790; aliyungf_tc=5b473c1317fb2ce5c71de79a0d3400e651ea02d7c903877bf6272618c8f50ecb; csrfToken=fDeyRWPLHWAd9SzDIwPjx_Vo; Hm_lpvt_ded2b36577e77da320c91b90209bf7ac=1624323452; tyc-user-info={%22state%22:%220%22%2C%22vipManager%22:%220%22%2C%22mobile%22:%2215901489692%22}; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTkwMTQ4OTY5MiIsImlhdCI6MTYyNDI2NzYxOSwiZXhwIjoxNjU1ODAzNjE5fQ.hgBPIwILNwtgjweoOGHVBnrH-cYgpjmyR590u__liS2MX00hDr2kSqFh9sMKyvL2MkIROe5f620GBrBze11pmQ',
        'Origin':'https://www.tianyancha.com',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
    }

    def get_url_list(self):
        url_list = []
        for i in['b5b65500011762','bc788500001211','b28b0500001209']:
            for j in range(5):
                url = self.url.format(i,j)
                url_list.append(url)
                return url_list


    def parse_url(self,url):
        resp = requests.get(url,headers=self.headers)
        response = resp.content.decode()
        resp_html = etree.HTML(response)
        div_list = resp_html.xpath("//div[@class='search-item sv-search-company']")
        url_content_list = []
        for div in div_list:
            item = {}
            item['brand_name'] = resp_html.xpath(".//div[@class='header']/a/text()")
            item['brand_biaoqian_all']=resp_html.xpath(".//div[@class = 'tag-list'] /a/text()")
            item['brand_url']=resp_html.xpath('//div[@class="header"]/a/@href')
            item['company_name']=resp_html.xpath('//div[@class="title -wider text-ellipsis"]/a/text()')
            item['company_url']=resp_html.xpath('//div[@class="title -wider text-ellipsis"]/a/@href')
            item['company_founddate ']= resp_html.xpath("//div[@class='title  text-ellipsis']/span/@title |//div[@class='info row text-ellipsis']/div[@class='']/span/text()")
            item['company_area']= resp_html.xpath("//div[contains(@class,'title  text-ellipsis') and contains(text(),'所属地')]/span/text()")
            print(item)
            url_content_list.append(item)
            return url_content_list

    # def parse_content(self,url_content_list):
    #
    #     content_list = []
    #
    #     for item in url_content_list:
    #
    #         url = item['url_content']
    #
    #         resp = requests.get(url,headers=self.headers)
    #
    #         response = resp.content.decode()
    #
    #         resp_html = etree.HTML(response)
    #         item['brand_name'] = resp_html.xpath(".//div[@class='header']/a/text()")
    #         item['brand_biaoqian_all']=resp_html.xpath(".//div[@class = 'tag-list'] /a/text()")
    #         item['brand_url']=resp_html.xpath('//div[@class="header"]/a/@href')
    #         item['company_name']=resp_html.xpath('//div[@class="title -wider text-ellipsis"]/a/text()')
    #         item['company_url']=resp_html.xpath('//div[@class="title -wider text-ellipsis"]/a/@href')
    #         item['company_founddate ']= resp_html.xpath("//div[@class='title  text-ellipsis']/span/@title |//div[@class='info row text-ellipsis']/div[@class='']/span/text()")
    #         item['company_area']= resp_html.xpath("//div[contains(@class,'title  text-ellipsis') and contains(text(),'所属地')]/span/text()")
    #         print(item)
    #         content_list.append(item)
    #         return content_list

    def save_content(self,url_content_list):
        with open('信息.txt','a+') as f:
            for content in url_content_list:
                f.write(json.dumps(content,ensure_ascii=False))
                f.write('\n')

    def run(self):

        url_list = self.get_url_list()

        for url in url_list:

            print(url)

            url_content_list = self.parse_url(url)

            # content_list = self.parse_content(url_content_list)

            self.save_content(url_content_list)

if __name__ == '__main__':
    tianyan = Tianyan()
    tianyan.run()
    print(item["brand_name"])

