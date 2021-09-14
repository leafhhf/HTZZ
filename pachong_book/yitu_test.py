import requests
from urllib.parse import urlencode
from pyquery import PyQuery as pq
#from pymongo import MongoClient


base_url = 'http://www.ocft.com/news/query?'
headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Host': 'www.ocft.com',
        'Connection': 'keep-alive',
        'Cookie': 'SESSIONID=5D48AA411B4991898BC2148C06F4D7A5; lang=; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=',
       # 'Referer': 'http://www.lianmenhu.com/apply/',
        'Referer': 'http://www.ocft.com/news',
        'Connection': 'keep-alive',
}
#client = MongoClient()
#db = client['weibo']
#collection = db['weibo']
#max_page = 10


def get_page(page):
    params = {
        'page': page,
        'category': ''

    }
    url = base_url + urlencode(params)
    print(url)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError as e:
        print('Error', e.args)


def parse_page(json):
    if json:

        items = json.get()
        for item in items:
            #item = item.get('mblog')
            ocft = {}
            ocft['title'] = item.get('newsTitle')
            ocft['content']= item.get('newsContent')
            ocft['report_time'] = item.get('publishTime')
            print(ocft)
            yield ocft


def save_to_mongo(result):
    if collection.insert(result):
        print('Saved to Mongo')


if __name__ == '__main__':
    for page in range(1,10):
        json = get_page(page)
        results = parse_page(json)



        for result in results:
            print(result)



            save_to_mongo(result)
