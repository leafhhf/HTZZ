# -*- coding: utf-8 -*-
import pymysql


def select_db(self, item, spider, db_table):
    cursor = self.conn.cursor()
    com = item['com']
    try:
        sql = "SELECT com FROM %s  where com ='%s'" % (db_table,com)

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
    params = [item['com'],item['current_url'],item['url'],item['com_jianjie'],item['totol_employee'],item['research_employee'],
                item['yearly_sales'],item['direct_export_experience'],item['synchronous_development_capability'],
                item['com_url'],item['directly_supporting'],item['first_major_customer'],item['first_major_customer_business_proportion'],
                item['second_major_customer'],item['second_major_customer_business_proportion'],item['third_major_customer'],
                item['third_major_customer_business_proportion'],item['indirect_supporting'],item['export_market'],item['primary_product'],
                item['taxpayer_registration_number'],item['reg_capital']]
    print('params',params)
    cursor = self.conn.cursor()
    try:
        com = self.cur.execute(
            'insert into gaishi_car_info (com,current_url,url,com_jianjie,totol_employee,research_employee,yearly_sales,direct_export_experience,\
                                        synchronous_development_capability,com_url,directly_supporting,first_major_customer,first_major_customer_business_proportion,\
                                        second_major_customer,second_major_customer_business_proportion,third_major_customer,third_major_customer_business_proportion,\
                                        indirect_supporting,export_market,primary_product,taxpayer_registration_number,reg_capital) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',params)
        # 'insert into ai_news_information_mid(title, url, reporttime, content, listpic1,  source) values (%s,%s,%s,%s,%s,%s)',params)
        self.conn.commit()
    except Exception as e:
        print("插入数据出错,错误原因%s" % e)
    #return item