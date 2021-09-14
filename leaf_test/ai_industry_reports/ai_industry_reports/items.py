# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AiIndustryReportsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    industryName = scrapy.Field()
    orgName = scrapy.Field()
    orgSName = scrapy.Field()
    publishDate = scrapy.Field()
    researcher = scrapy.Field()
    url = scrapy.Field()
    source = scrapy.Field()
    pdfurl = scrapy.Field()
    pass
