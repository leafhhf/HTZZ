# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from it_juzi.settings import DATABASE_DB, DATABASE_HOST, DATABASE_PORT, DATABASE_PWD, DATABASE_USER
import pymysql

class ItjuziPipeline(object):
    def __init__(self):
        host = DATABASE_HOST
        port = DATABASE_PORT
        user = DATABASE_USER
        passwd = DATABASE_PWD
        db = DATABASE_DB
        try:
            self.conn = pymysql.Connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8')
        except Exception as e:
            print("连接数据库出错,错误原因%s"%e)
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        params = [item['com_id'],item['com_name'], item['com_registered_name'], item['com_des'], item['com_scope'],
                  item['prov'], item['city'], item['round'], item['money'], item['invse_company'],item['invse_des'],item['invse_time'],item['invse_title']]
        try:
            com = self.cur.execute(
                'insert into itjuzi_tourongzi_python(com_id,com_name, com_registered_name, com_des, com_scope, prov, city, round, money, invse_company, invse_des, invse_time, invse_title)values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',params)
            self.conn.commit()
        except Exception as e:
            print("插入数据出错,错误原因%s" % e)
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()