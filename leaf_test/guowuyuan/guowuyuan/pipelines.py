# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from guowuyuan.mysql_config import insert_db
from guowuyuan.mysql_config import select_db
import pymysql
from redis import Redis
class GuowuyuanPipeline(object):
    def __init__(self):
    # 连接数据库
    # spider数据库中 只有一个表为mydb，表中有两个字段title和keywd
         self.conn = pymysql.Connect(host="172.16.16.152", user="root", passwd="123456", db="kejibu",
                                charset='utf8')

    def process_item(self, item, spider):
        # 将获取到的name和keywd分别赋给变量name和变量keywd
        db_table='guowuyuan'
        self.cur = self.conn.cursor()
        url = item['url']
        title = item['title']
        issue_db = select_db(self,item, spider, db_table)
        print('issue_db',issue_db,url)
      #  print('title', title)

        #select_db(self, item, spider, db_table)
       # (self, db_table, item, spider):
        try:
            if issue_db == url:
             #   insert_db(self, db_table, item, spider)
                print('%s 已经存在！' % url)
            else:
                insert_db(self, db_table, item, spider)
                print('添加%s到%s成功' % (title, db_table))
        except Exception as e:
            print('%s 已经存在！' % title)
            print(e)
        # 提交
        self.conn.commit()
      #  return item

    def close_spider(self, spider):
    # 关闭数据库连接
        self.conn.close()
