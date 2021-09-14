# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GuowuyuanItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    publishTime = scrapy.Field()
    summary = scrapy.Field()
    url = scrapy.Field()
    pcode = scrapy.Field()
    ptime = scrapy.Field()
    source = scrapy.Field()
    childtype = scrapy.Field()
    puborg = scrapy.Field()
    subjectword = scrapy.Field()
    colname = scrapy.Field()
    content = scrapy.Field()
