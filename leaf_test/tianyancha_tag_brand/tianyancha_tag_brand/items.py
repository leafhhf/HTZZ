# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TianyanchaTagBrandItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    tag= scrapy.Field()
    brand_number= scrapy.Field()
    brand= scrapy.Field()
    brand_url = scrapy.Field()
    brand_biaoqian_all = scrapy.Field()
    company= scrapy.Field()
    company_url= scrapy.Field()
    found_date= scrapy.Field()
    province= scrapy.Field()
    page= scrapy.Field()

    pass
