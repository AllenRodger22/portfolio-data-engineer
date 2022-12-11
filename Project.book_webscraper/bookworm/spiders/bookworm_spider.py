import scrapy


class BookwormSpiderSpider(scrapy.Spider):
    name = 'bookworm_spider'
    start_urls = ['https://books.toscrape.com/']
    data = {"name":[],"price":[]}
    counter = 1
    urls = []
    for i in range(50):
        urls.append(start_urls[0]+ f"/catalogue/page-{i+1}.html")
    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url=url, callback = self.parse)

    def parse(self, response):
        if self.counter == 50:
            yield self.data
        else:
            self.counter = self.counter + 1
            for n in range(20):
                self.data["name"].append(response.xpath(f'//*[@id="default"]/div/div/div/div/section/div[2]/ol/li[{n+1}]/article/h3/a/@title').get())
                self.data["price"].append(response.xpath(f'//*[@id="default"]/div/div/div/div/section/div[2]/ol/li[{n+1}]/article/div[2]/p[1]/text()').get()[1:])

        