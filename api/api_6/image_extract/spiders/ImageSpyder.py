import scrapy
from scrapy.http.request import Request
from scrapy.loader import ItemLoader
from typing import TYPE_CHECKING, Any, cast
from image_extract.items import ImageExtractItem


class ImagespyderSpider(scrapy.Spider):
    name = "ImageSpyder"
    allowed_domains = ["unsplash.com"]
    start_urls = ["https://unsplash.com"]

    def start_requests(self):
        return [scrapy.Request(self.start_urls[0], callback = self.parse_category)]

    def parse_category(self, response):
        print("+++++++++++++++++")
        print(self.start_urls)
        cat_list = {}
        cat_links = {}
        selector = scrapy.selector.Selector(response=response)
        links_categories = selector.xpath("//a[@class='wuIW2 R6ToQ']/@href").getall()
        for number, i in enumerate(links_categories):
            categories = i.split('/')[-1]
            cat_list[f'{number}'] = categories
            cat_links[f'{categories}'] = i
        print_cat_value = 'Input number of categiries:'
        print('*'*len(print_cat_value))
        print(f'{print_cat_value}')
        print('*'*len(print_cat_value))
        categories_values = cat_list[input(f'{cat_list}')]
        categories_image_value= int(input('how many pictures do you want to upload in numbers?'))
        yield response.follow(cat_links[f'{categories_values}'], callback = self.parse_image_from_categories, meta = {
             'categories_image_value': categories_image_value,
             'categories': categories})

    def parse_image_from_categories(self, response):           
            result = {}
            categories_image_value = response.meta.get('categories_image_value')
            categories = response.meta.get('categories')
            selector = scrapy.selector.Selector(response=response)
            elements = selector.xpath("//img[@class='I7OuT DVW3V L1BOa']")
            if len(elements) < categories_image_value:
                 categories_image_value = len(elements)
            for num, element in enumerate(elements[:categories_image_value]):
                image_url = element.xpath("@src").get()
                title = element.xpath("@alt").get(default=f'{categories}_{num}')
                result[f'{title}']={'link':image_url, 'category':categories}
            yield result