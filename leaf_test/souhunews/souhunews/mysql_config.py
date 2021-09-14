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
    source = '搜狐新闻人工智能+'+item["authorName"]
    #item['content']
    params = [item['title'], item['url'], item['reporttime'],item['content'],item['content_img'],source]
    print('params',params)
    cursor = self.conn.cursor()
    try:
        com = self.cur.execute(
            'insert into souhu_news_information(title, url, reporttime, content, listpic1, source) values (%s,%s,%s,%s,%s,%s)',params)
        # 'insert into ai_news_information_mid(title, url, reporttime, content, listpic1,  source) values (%s,%s,%s,%s,%s,%s)',params)
        self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item