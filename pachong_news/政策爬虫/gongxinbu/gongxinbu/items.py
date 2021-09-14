# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GongxinbuItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    pubjigou = scrapy.Field()
    themename = scrapy.Field()
    filenumbername = scrapy.Field()
    puborg = scrapy.Field()
    publishTime = scrapy.Field()
    createdate = scrapy.Field()
    jsearch_date = scrapy.Field()
    columnname = scrapy.Field()
    url = scrapy.Field()
    content = scrapy.Field()
