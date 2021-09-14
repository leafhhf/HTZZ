# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class HuaweicloudNewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    content = scrapy.Field()
    report_time = scrapy.Field()
    content_img = scrapy.Field()
    pic1_url = scrapy.Field()
    pic2_url = scrapy.Field()
    pic3_url = scrapy.Field()
    pass
