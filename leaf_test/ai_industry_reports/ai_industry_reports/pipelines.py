# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from ai_industry_reports.mysql_config import insert_db
from ai_industry_reports.mysql_config import select_db
import pymysql




class AiIndustryReportsPipeline:
    def process_item(self, item, spider):
        return item
    # -*- coding: utf-8 -*-

    def __init__(self):
        # 连接数据库
        # spider数据库中 只有一个表为mydb，表中有两个字段title和keywd
        self.conn = pymysql.connect(host="172.16.16.152", user="root", passwd="123456", db="ly",
                                    charset='utf8')
        #self.conn = pymysql.connect(host="10.10.1.51", user="root", passwd="tianzhi", db="knowledge_graph",
                                   # charset='utf8')


    def process_item(self, item, spider):
        self.cur = self.conn.cursor()
        db_table = 'ai_industry_reports_mid'

        params = [item['title'], item['orgName'], item['orgSName'], item['publishDate'], item['industryName'],
                  item['researcher'], item['url'], item['source'], item['pdfurl']]
        print('params', params)
        cursor = self.conn.cursor()
        title = item['title']
        issue_db = select_db(self, item, spider, db_table)
        print('issue_db', issue_db)
        print('title', title)
        try:
            if issue_db:
                # insert_db(self, db_table, item, spider)

                print('%s 已经存在！' % title)
            else:
                insert_db(self, db_table, item, spider)
                print('添加%s到%s成功' % (title, db_table))
        except Exception as e:
            print('程序报错')
            print(e)
