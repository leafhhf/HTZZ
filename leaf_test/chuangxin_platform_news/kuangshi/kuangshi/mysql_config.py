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


    params = [item['title'],item['url'], item['report_time'],item['content'],"图像感知平台",'北京旷视科技有限公司',item['pic1_url'],item['pic2_url'],item['pic3_url']]
    #print('params',params)
    cursor = self.conn.cursor()
    try:
        com = self.cur.execute(
            'insert into ai_chuangxin_platform_news_kuangshi(title, platform_news_url, report_time, content, platform_name,company_name,pic1_url,pic2_url,pic3_url) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            params)
        self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item
