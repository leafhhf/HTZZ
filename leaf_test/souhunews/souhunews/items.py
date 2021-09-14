# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SouhunewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id = scrapy.Field()
    authorId = scrapy.Field()
    authorName = scrapy.Field()
    picUrl = scrapy.Field()
    images = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    reporttime = scrapy.Field()
    content = scrapy.Field()
    content_img = scrapy.Field()


