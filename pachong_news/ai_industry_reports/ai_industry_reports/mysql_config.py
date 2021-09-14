# -*- coding: utf-8 -*-
import pymysql


def select_db(self, item, spider, db_table):
    cursor = self.conn.cursor()
    title = item['title']
    try:
        sql = "SELECT title FROM %s  where title ='%s'" % (db_table,title)

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

    self.cur = self.conn.cursor()
#,item['content'],item['content_img']
   # print('content111111111',item['content'])
  #  print('content_img111111111', item['content_img'])
    params = [item['title'], item['orgName'], item['orgSName'], item['publishDate'], item['industryName'],
              item['researcher'], item['url'], item['orgName'], item['pdfurl']]
    print('params',params)
    cursor = self.conn.cursor()
    try:
        com = self.cur.execute(
            'insert into ai_industry_reports_mid(title, orgName, orgSName, reporttime, industryName,  researcher, url,source,pdfurl) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            params)
        self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item

