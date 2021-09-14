# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class QukuailianItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    imgurl = scrapy.Field()
    summary = scrapy.Field()
    url = scrapy.Field()
    reporttime = scrapy.Field()
    content = scrapy.Field()
    content_img = scrapy.Field()

    pass
