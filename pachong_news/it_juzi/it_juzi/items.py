# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ItjuziItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    com_id = scrapy.Field()
    com_name = scrapy.Field()
    com_registered_name = scrapy.Field()
    com_des = scrapy.Field()
    invse_des = scrapy.Field()
    invse_title = scrapy.Field()
    money = scrapy.Field()
    name = scrapy.Field()
    prov = scrapy.Field()
    round = scrapy.Field()
    invse_time = scrapy.Field()
    city = scrapy.Field()
    com_registered_name = scrapy.Field()
    com_scope = scrapy.Field()
    invse_company = scrapy.Field()