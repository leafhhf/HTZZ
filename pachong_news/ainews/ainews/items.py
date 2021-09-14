# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AInewsItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    imgurl = scrapy.Field()
    type = scrapy.Field()
    keyword = scrapy.Field()
    url = scrapy.Field()
    reporttime = scrapy.Field()
    source = scrapy.Field()
    content = scrapy.Field()
    content_img = scrapy.Field()
    summery = scrapy.Field()
