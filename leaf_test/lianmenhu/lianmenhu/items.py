# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class LianmenhuItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
   # imgurl = scrapy.Field()
    summary = scrapy.Field()
    url = scrapy.Field()
    reporttime = scrapy.Field()
    content = scrapy.Field()
    content_img = scrapy.Field()
