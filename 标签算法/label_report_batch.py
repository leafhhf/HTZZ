"""
给公司专利打上标签
"""

"""

1.获取公司专利表
2.对每一行的，遍历nodes.json
3、按照合并结果进行插入
"""

import time
import json
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from dbutils.pooled_db import PooledDB

pool = PooledDB(creator=pymysql,
                maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
                mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
                maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
                maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
                blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
                host='172.16.16.100',  # 数据库ip地址
                port=3306,
                user='root',
                passwd='tianzhi',
                db='knowledge_graph',
                use_unicode=True,
                charset='utf8')

executor = ThreadPoolExecutor(max_workers=8)
with open("./nodes.json", mode="r", encoding="utf-8") as f:
    nodes_json = f.read()

nodes = json.loads(nodes_json)
nodes_dic = {node["id"]: node["path"] for node in nodes}
nodes_name_dic = {node["id"]: node["name"] for node in nodes}

def do_thread(page, start_num):
    end_num = start_num + 1000
    print("当前处理到第%s页,开始数字是%s,结束数字是%s" % (page, start_num, end_num))

    # you can do something here
    db = pool.connection()  # 连接数据池
    cursor = db.cursor()  # 获取游标

    # 1.获取新闻报告表
    sql = """
    select b.id,b.title from (select id from `knowledge_graph`.`ai_industry_reports_mid` WHERE id > %s and id <= %s) a 
    join `knowledge_graph`.`ai_industry_reports_mid` b on a.id = b.id;
    """

    cursor.execute(sql, (start_num, end_num))
    results = cursor.fetchall()

    def check_text_label(text):
        level = []
        for node in nodes:
            introduction = node["introduction"]
            if any([word in text for word in introduction if word]):
                level.append({
                    "id": node["id"],
                    "name": node["name"],
                    "ceng_ji": node["ceng_ji"],
                })

        classify = list(set([item["name"] for item in level]))
        return classify, level

    data = []
    for row in results:
        _id, title = row
        text = title
        _classify, _level = check_text_label(text)
        if not _classify or not _level:
            continue

        _classify.insert(0, "数字经济")
        _classify = ",".join(_classify)
        # _classify, _level = _classify, json.dumps(_level)
        labels = list(set([nodes_dic[l["id"]][1] for l in _level]))
        labels = list([nodes_name_dic[l] for l in labels])
        labels = ",".join(labels)

        data.append((labels, _classify, _id))

    sql_update = """
    UPDATE `knowledge_graph`.`ai_industry_reports_mid` SET industry = %s , detail_field = %s WHERE id = %s;
    """

    try:
        # 查询 id ,key_words DE
        cursor.executemany(sql_update, data)
        # 每1w条提交一次
        db.commit()
    except Exception as e:
        print("SQL ERROR！", e)
        db.rollback()
    finally:
        cursor.close()
        db.close()
    return id


if __name__ == "__main__":
    start_time = time.time()
    tasks = []

    db = pool.connection()  # 连接数据池
    cursor = db.cursor()  # 获取游标

    sql = """
    SELECT last_update_id FROM ai_industry_reports_mid_last_update_id;
    """
    cursor.execute(sql)
    last_update_id = cursor.fetchone()[0]

    sql = """
    SELECT max(id) FROM ai_industry_reports_mid;
    """
    cursor.execute(sql)
    max_id = cursor.fetchone()[0]

    # 一共 27236796 条数据，每个线程每次执行10W条
    all_count = max_id - last_update_id
    interval = 1000
    pages = all_count // interval + 1  # 总页数,63987页

    for i in range(1, pages + 1):
        start_num = (i - 1) * interval + last_update_id  # 起始个数
        tasks.append(executor.submit(do_thread, i, start_num))  # submit函数来提交线程需要执行的任务（函数名和参数）到线程池中，不阻塞

    datas = []
    for future in as_completed(tasks):  # as_completed()是ThreadPoolExecutor中的方法，用于取出所有任务的结果
        datas.append(future.result())

    sql_update = """
    UPDATE `knowledge_graph`.`ai_industry_reports_mid_last_update_id` SET last_update_id = %s;
    """
    cursor.execute(sql_update, max_id)
    db.commit()

    cursor.close()
    db.close()
    print("8个线程配合连接池插入800总共用时:" + str(time.time() - start_time))  # 100000 * 100
    print("all end !!!")

    # 8个线程配合连接池插入1000*1000总共用时:36.32487964630127
    # 8个线程配合连接池插入12963030总共用时:605.9726493358612
