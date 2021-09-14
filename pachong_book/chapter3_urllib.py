import urllib.request

response = urllib.request.urlopen('http://www.python.org')
print(response.read().decode('utf-8'))
print(type(response))

print(response.status)
print(response.getheaders())
print(response.getheader('Server'))


import urllib.parse
import urllib.request
data = bytes(urllib.parse.urlencode({'word':'hello'}),encoding='utf8')
response = urllib.request.urlopen('http://httpbin.org/post',data=data)
print(response.read())


import urllib.request
response = urllib.request.urlopen('http://httpbin.org/get',timeout=1)
print(response.read())


from urllib import request,parse

url = 'http://httpbin.org/post'
header = {'User'}




import  requests
r=  requests.get('http://www.baidu.com/')
print(type(r))
print(r.status_code)
print(type(r.text))
print(r.text)
print(r.cookies)


r= requests.get('http://httpbin.org/get')
print(r.text)

import requests

data = {'name':'germy','age':'22'}
r = requests.get('http://httpbin.org/get',params = data)
print(r.text)
print(r.json())



import requests
import re

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
}
r = requests.get("https://www.zhihu.com/explore", headers=headers)
pattern = re.compile('explore-feed.*?question_link.*?>(.*?)</a>', re.S)
titles = re.findall(pattern, r.text)
print(titles)


import requests

r = requests.get("https://github.com/favicon.ico")
print(r.text)
print(r.content)


import requests

r = requests.get("https://github.com/favicon.ico",timeout=5)
with open('favicon.ico', 'wb') as f:
    f.write(r.content)







import http.cookiejar, urllib.request

cookie = http.cookiejar.CookieJar()
handler = urllib.request.HTTPCookieProcessor(cookie)
opener = urllib.request.build_opener(handler)
response = opener.open('http://www.baidu.com')
for item in cookie:
    print(item.name+"="+item.value)

from urllib.parse import urlparse

result = urlparse('http://www.baidu.com/index.html;user?id=5#comment')
print(type(result), result)

























