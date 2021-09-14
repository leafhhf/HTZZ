# -*- coding: utf-8 -*-
import pymysql


def select_db(self, item, spider, db_table):
    cursor = self.conn.cursor()
    # title = item['title']
    url = item['url']
    try:
        sql = "SELECT news_url FROM %s  where news_url ='%s'" % (db_table,url)
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
    try:
       # self.conn = pymysql.Connect(host="172.16.16.152", user="root", passwd="123456", db="knowledge_graph", charset='utf8')
       self.conn = pymysql.Connect(host="172.16.16.100", user="root", passwd="tianzhi", db="rawdata",
                                   charset='utf8')
    except Exception as e:
        print("连接数据库出错,错误原因%s" % e)
    self.cur = self.conn.cursor()
#,item['content'],item['content_img']
   # print('content111111111',item['content'])
  #  print('content_img111111111', item['content_img'])

    params = [item['title'],item['url'], item['report_time'],item['content'],"美国财政部treasury_ofac_ra",'美国','美国财政部','1']
    #print('params',params)
    cursor = self.conn.cursor()
    try:
        com = self.cur.execute(
            'insert into block_lawsuit_incident_treasury_ofac_ra(news_title, news_url, reporttime, content, source,country,institution,state) values (%s,%s,%s,%s,%s,%s,%s,%s)',
            params)
        self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item
