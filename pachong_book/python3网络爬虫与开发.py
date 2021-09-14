

# -------------------------------------------
from selenium import webdriver
browser = webdriver.Chrome()
# --------------------------------------------
from selenium import webdriver
browser = webdriver.Firefox()
#--------------------------------------------
from selenium import webdriver
browser = webdriver.PhantomJS()
browser.get('http://baidu.com')
print(browser.current_url)
# -------------------------------------------
import lxml
from bs4 import BeautifulSoup
soup = BeautifulSoup('<p>Hello</p>','lxml')
print(soup.p.string)
# -------------------------------------------
import pyquery
# -------------------------------------------
import pytesseract
from PIL import Image
image = Image.open("C:/Users/Administrator/Desktop/python_work/pachong/image.png")
print(pytesseract.image_to_string(image))

#--------------------------------------------------------------------------------------

#mongod --bind_ip 0.0.0.0 --logpath "C:\Program Files\MongoDB\Server\4.4\logs\mongodb.log" --logappend --dbpath "C:\Program Files\MongoDB\Server\4.4\data\db" --port 27017 --serviceName "MongoDB" --serviceDisplayName "MongoDB" --install

from flask import Flask
app = Flask(__name__)
@app.route("/")
def hello():
    return "Hello World"
if __name__ == "__main__":
    app.run()
# ---------------------------------------------------------



import platform

if platform.system() == "Windows":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# ---------------------------------------------------------
import tornado.ioloop
import tornado.web


class MainHanderler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello World")


def make_app():
    return tornado.web.Application([(r"/", MainHanderler), ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8000)
    tornado.ioloop.IOLoop.current().start()

# ------------------------------------------------------------------------------------------------------------------
import pycurl
import pyspider
import scrapy_redis


#   scrapy shell http://doc.scrapy.org/en/latest/_static/selectors-sample1