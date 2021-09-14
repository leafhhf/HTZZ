# -*- coding: utf-8 -*-
import pymysql


def select_db(self, item, spider, db_table):
    cursor = self.conn.cursor()
    title = item['title']
    try:
        sql = "SELECT title FROM %s  where title ='%s'" % (db_table,title)

        cursor.execute(sql)
        result = cursor.fetchone()
        result = result[0]
       # print('打印', result)
        self.conn.commit()
    except ValueError as e:
        print(e)
        self.conn.rollback()
    finally:
        return result


def insert_db(self, db_table, item, spider):
#    try:
#        self.conn = pymysql.Connect(host="172.16.16.97", user="root", passwd="123456", db="knowledge_graph", charset='utf8')
#    except Exception as e:
#        print("连接数据库出错,错误原因%s" % e)
    self.cur = self.conn.cursor()

    params = [item['title'], item['summary'], item['content_img'], item['imgurl'], item['url'], item['reporttime'],item['content'],item['type'],"数字经济网linkgapi"]
    print('params',params)
    cursor = self.conn.cursor()
    try:
        com = self.cur.execute(
            'insert into ai_news_information_mid(title, summary, listpic1, listpic2, url, reporttime, content, type, source) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            params)
        self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item
