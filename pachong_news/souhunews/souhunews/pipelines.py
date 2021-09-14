# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from souhunews.mysql_config import insert_db
from souhunews.mysql_config import select_db
import pymysql

class SouhunewsPipeline(object):
    def process_item(self, item, spider):
        return item

    def __init__(self):
    # 连接数据库
    # spider数据库中 只有一个表为mydb，表中有两个字段title和keywd
        self.conn = pymysql.connect(host="172.16.16.152", user="root", passwd="123456", db="ly",charset='utf8')

        # host = "10.10.1.51", user = "root", passwd = "tianzhi", db = "knowledge_graph"
      #  self.conn = pymysql.connect(host="172.16.16.152", user="root", passwd="123456", db="knowledge_graph", charset='utf8')

    def process_item(self, item, spider):
        # 将获取到的name和keywd分别赋给变量name和变量keywd
        db_table='ai_news_information_mid'
        self.cur = self.conn.cursor()

        # 可能存在没有关键词的情况 如果直接填入item["keywd"][0]可能会出现数组溢出的情况
        url = item['url']
        title = item['title']
        issue_db = select_db(self,item, spider, db_table)
      #  print('issue_db',issue_db)
      #  print('title', title)

        #select_db(self, item, spider, db_table)
       # (self, db_table, item, spider):
        try:
            if issue_db == title:
               # insert_db(self, db_table, item, spider)
                print('%s 已经存在！' % title)
            else:
                insert_db(self, db_table, item, spider)
                print('添加%s到%s成功' % (title, db_table))
        except Exception as e:
            print('%s 已经存在！' % title)
            print(e)

        # 通过query实现执行对应的sql语句
        '''
        try:
            com = self.cur.execute(
                'insert into AI_news_information_mid(title, keyword, type, listpic1, url, reporttime)values (%s,%s,%s,%s,%s,%s)',params)
            self.conn.commit()
        except Exception as e:
            print("插入数据出错,错误原因%s" % e)
            print("插入数据出错,错误原因%s" % e)
        return item

        self.conn.query(sql)
        '''
        # 提交
        self.conn.commit()
      #  return item

    def close_spider(self, spider):
    # 关闭数据库连接
        self.conn.close()
