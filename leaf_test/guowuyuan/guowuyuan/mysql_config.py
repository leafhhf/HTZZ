# -*- coding: utf-8 -*-
import pymysql


def select_db(self, item, spider, db_table):
    cursor = self.conn.cursor()
    url = item['url']
    try:
        sql = "SELECT url FROM %s  where url ='%s'" % (db_table,url)

        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            result = result[0]
       # print('打印', result)
        self.conn.commit()
        return result
    except ValueError as e:
        print(e)
        self.conn.rollback()
   # finally:



def insert_db(self, db_table, item, spider):
    try:
       # self.conn = pymysql.Connect(host="10.10.1.51", user="root", passwd="tianzhi", db="knowledge_graph", charset='utf8')
       self.conn = pymysql.Connect(host="172.16.16.152", user="root", passwd="123456", db="kejibu",
                                   charset='utf8')
    except Exception as e:
        print("连接数据库出错,错误原因%s" % e)
    self.cur = self.conn.cursor()

    params = [item['title'], item['publishTime'],item['summary'],item['url'], item['pcode'],item['ptime'],item['source'],item['childtype'],item['puborg'],item['subjectword'],item['colname'],item['content']]
  #  print('params',params)
   # cursor = self.conn.cursor()
    try:
        self.cur.execute(
            'insert into guowuyuan(title,publishTime,summary,url,pcode,ptime,source,childtype ,puborg ,subjectword,colname,content) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',params)
        self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item

