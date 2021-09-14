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
        cursor = self.conn.cursor()
        sql = "select id from %s order by (id+0) desc limit 1" % (db_table)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            result = result[0]
        print("已读取id:{}".format(result))
        # result = result[0]
        if result is None :
            result = 1
            id =str(result)
            params =[id,item['issuer'],item['policy_number'],item['public_date'],item['text'],
                    item['title'],item['title_class'],item['url'],"中华人民共和国中央人民政府网"]
            print('params',params)
            cursor = self.conn.cursor()
            com = self.cur.execute(
                'insert into rm_policy_publication_crawl (id,issuer,policy_number,public_date,text,title,title_class,data_source,crawl_web_name) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',params)
            self.conn.commit()
        else:
            id = str(int(result) + 1)
            params = [str(id),item['issuer'], item['policy_number'], item['public_date'], item['text'],
                      item['title'], item['title_class'], item['url'], "中华人民共和国中央人民政府网"]
            print('params', params)
            cursor = self.conn.cursor()
            com = self.cur.execute(
                'insert into rm_policy_publication_crawl (id,issuer,policy_number,public_date,text,title,title_class,data_source,crawl_web_name) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                params)
            self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item