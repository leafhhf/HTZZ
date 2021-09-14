# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GasgooItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    current_url = scrapy.Field()
    com = scrapy.Field()
    url = scrapy.Field()
    com_jianjie = scrapy.Field()
    totol_employee = scrapy.Field()
    research_employee = scrapy.Field()
    yearly_sales = scrapy.Field()
    direct_export_experience = scrapy.Field()
    synchronous_development_capability = scrapy.Field()
    com_url = scrapy.Field()
    directly_supporting = scrapy.Field()
    first_major_customer = scrapy.Field()
    first_major_customer_business_proportion = scrapy.Field()
    second_major_customer = scrapy.Field()
    second_major_customer_business_proportion = scrapy.Field()
    third_major_customer = scrapy.Field()
    third_major_customer_business_proportion = scrapy.Field()
    indirect_supporting = scrapy.Field()
    export_market = scrapy.Field()
    primary_product = scrapy.Field()
    taxpayer_registration_number = scrapy.Field()
    reg_capital = scrapy.Field()
    pass
