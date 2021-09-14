import scrapy


class OfacSpider(scrapy.Spider):
    name = 'ofac'
    allowed_domains = ['home.treasury.gov']
    start_urls = ['http://home.treasury.gov/']

    def parse(self, response):
        pass
