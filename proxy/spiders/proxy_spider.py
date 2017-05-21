import scrapy 

class ProxySpider(scrapy.Spider):
    page=10
    urls=['http://www.kuaidaili.com/proxylist/{}/'.format(i) for i in range(1, page + 1)]
    for url in urls:
        yield scrapy.Request(url)

    def parse(self,response):
        



        


