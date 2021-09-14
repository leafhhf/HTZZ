# -*- coding: utf-8 -*-
import scrapy
from it_juzi.items import ItjuziItem
from it_juzi.settings import JUZI_PWD, JUZI_USER
import json


class JuziSpider(scrapy.Spider):
    name = 'juzi'
    allowed_domains = ['itjuzi.com']

    def start_requests(self):
        """
        先登录桔子网
        """
        header = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
            "Host": "www.itjuzi.com",
            "Referer": "https://www.itjuzi.com/investevent",
        }
        url = "https://www.itjuzi.com/api/authorizations"
        #url = "https://www.itjuzi.com/login"
        payload = {"account": JUZI_USER, "password": JUZI_PWD}
        # 提交json数据不能用scrapy.FormRequest，需要使用scrapy.Request，然后需要method、headers参数
        yield scrapy.Request(url=url,
                             method="POST",
                             body=json.dumps(payload),
                             headers=header,
                             callback=self.parse
                             )

    def parse(self, response):
        # 获取Authorization参数的值
        token = json.loads(response.text)
        url = "https://www.itjuzi.com/api/investevents"
        header = {
            "Content-Type": "application/json",
            "Authorization": token,
            "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
            "Host": "www.itjuzi.com",
            "Referer": "https://www.itjuzi.com/investevent",
        }
        payload = {
            "pagetotal": 491, "total": 491, "per_page": 20, "page": 1, "type": 1, "scope": "", "sub_scope": "",
            "round": [], "valuation": [], "tag":"",
            "valuations": "", "ipo_platform": "", "equity_ratio": "", "status": "", "prov": "", "city": [],
            "time": [2020], "selected": "", "location": "国内", "hot_city": "", "currency": [], "keyword": ""
        }
        #[2020]
        yield scrapy.Request(url=url,
                             dont_filter=True,
                             method="POST",
                             body=json.dumps(payload),
                             meta={'token': token},
                             # 把Authorization参数放到headers中
                             headers={'Content-Type': 'application/json', 'Authorization': token['data']['token']},
                             callback=self.parse_info
                             )

    def parse_info(self, response):
        # 获取传递的Authorization参数的值
        token = response.meta["token"]
        # 获取总记录数
        total = json.loads(response.text)["data"]["page"]["total"]
        # 因为每页20条数据，所以可以算出一共有多少页
        if type(int(total) / 20) is not int:
            page = int(int(total) / 20) + 1
        else:
            page = int(total) / 20

        url = "https://www.itjuzi.com/api/investevents"
       # page + 1
        for i in range(0, 10):
            payload = {
                "pagetotal": 491, "total": 491, "per_page": 20, "page": i, "type": 1, "scope": "", "sub_scope": "",
                 "round": [], "valuation": [],"tag":"",
                 "valuations": "", "ipo_platform": "", "equity_ratio": "", "status": "", "prov": "", "city": [],
                 "time": [2020], "selected": "", "location": "国内", "hot_city": "", "currency": [], "keyword": ""
            }


            yield scrapy.Request(url=url,
                                 method="POST",
                                 body=json.dumps(payload),
                                 headers={'Content-Type': 'application/json', 'Authorization': token['data']['token']},
                                 callback=self.parse_detail
                                 )

    def parse_detail(self, response):
        infos = json.loads(response.text)["data"]["data"]
        for i in infos:
            item = ItjuziItem()
            item["com_id"] = i["com_id"]
            item["invse_des"] = i["invse_des"]
            item["com_des"] = i["com_des"]
            item["invse_title"] = i["invse_title"]
            item["money"] = i["money"]
            item["com_name"] = i["name"]
            item["prov"] = i["prov"]
            item["round"] = i["round"]
            item["invse_time"] = str(i["year"]) + "-" + str(i["month"]) + "-" + str(i["day"])
            item["city"] = i["city"]
            item["com_registered_name"] = i["com_registered_name"]
            item["com_scope"] = i["com_scope"]
            invse_company = []
            for j in i["investor"]:
                invse_company.append(j["name"])
            item["invse_company"] = ",".join(invse_company)
            yield item
