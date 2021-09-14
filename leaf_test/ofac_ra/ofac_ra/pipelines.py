# -*- coding: utf-8 -*-

import pymysql
from ofac_ra.mysql_config import insert_db
from ofac_ra.mysql_config import select_db
import pymysql
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


class OfacRaPipeline(object):

    def __init__(self):
        # 连接数据库
        # spider数据库中 只有一个表为mydb，表中有两个字段title和keywd
      #  self.conn = pymysql.connect(host="172.16.16.152", user="root", passwd="123456", db="knowledge_graph", charset='utf8')
        self.conn = pymysql.connect(host="172.16.16.100", user="root", passwd="tianzhi", db="rawdata",
                                    charset='utf8')

    def process_item(self, item, spider):
        # 将获取到的name和keywd分别赋给变量name和变量keywd
        db_table = 'block_lawsuit_incident_treasury_ofac_ra'
        self.cur = self.conn.cursor()
        # insert_db(self, db_table, item, spider)
        # title = item['title']
        url = item['url']
        # print('添加%s到%s成功' % (title, db_table))
        # 可能存在没有关键词的情况 如果直接填入item["keywd"][0]可能会出现数组溢出的情况
        # title = item['title']
        issue_db = select_db(self, item, spider, db_table)
        #
        # print('title', title)
        # #  print('title', title)
        #
        # # select_db(self, item, spider, db_table)
        # # (self, db_table, item, spider):
        try:
            print('issue_db', issue_db)
            if issue_db is None:
                print('aa')
                insert_db(self, db_table, item, spider)
                print('添加%s到%s成功' % (url, db_table))
            else:
                print('%s 已经存在！' % url)
        except Exception as e:
            print(e)
        # 提交
        self.conn.commit()
        #  return item

    def close_spider(self,spider):
        # 关闭数据库连接
        self.conn.close()
