import scrapy
from scrapy.selector import Selector
import os
from dotenv import load_dotenv
load_dotenv()

from scrapy.http import XmlResponse
from pubmed_api.items import PubmedApiItem

api_key = os.getenv('api_key')


class PubmedSpider(scrapy.Spider):
    name = "pubmed"
    allowed_domains = ["eutils.ncbi.nlm.nih.gov"]
    start_urls = ["https://eutils.ncbi.nlm.nih.gov"]
    api_key = api_key
    query_param = '/entrez/eutils/'
    query = 'stroke[mesh]+AND+actilyse[mesh]+AND+2023:2024[pdat]'
    db = 'pubmed'
    retmode = 'xml'

    def start_requests(self):
        start_urls = self.start_urls[0] + self.query_param + f'esearch.fcgi?db={self.db}&term={self.query}&usehistory=y&api_key={self.api_key}'
        return [scrapy.FormRequest(start_urls, callback = self.parse_al_id)]

    def parse_al_id(self, response):
        selector = Selector(response=response)
        result = {
        'Count': selector.xpath('/eSearchResult/Count/text()').get(),
        'RetMax': selector.xpath('/eSearchResult/RetMax/text()').get(),
        'RetStart': selector.xpath('/eSearchResult/RetStart/text()').get(),
        'QueryKey': selector.xpath('/eSearchResult/QueryKey/text()').get(),
        'WebEnv': selector.xpath('/eSearchResult/WebEnv/text()').get(),
        'IdList': selector.xpath('/eSearchResult/IdList').getall(),
        }
        retmax = result['Count']
        url = f'{response.url}&retmax={retmax}'
        yield scrapy.Request(url, callback=self.parse_id_link)

    def parse_id_link(self, response):
        selector = Selector(response=response)
        ids = set(selector.xpath('/eSearchResult/IdList/Id/text()').getall())
        for id in ids:
            url_id = self.start_urls[0] + self.query_param + f'efetch.fcgi?db={self.db}&id={id}&retmode={self.retmode}&api_key={self.api_key}'
            yield response.follow(url_id, callback=self.parse_article)

    def parse_article(self, response):
                result_abstract = {}
                result_abstract['OBJECTIVES'] = response.xpath("//AbstractText [@Label = 'OBJECTIVES' or @Label = 'OBJECTIVE']/text()").get()
                result_abstract['BACKGROUND'] = response.xpath("//AbstractText [@Label = 'BACKGROUND']/text()").get()
                result_abstract['INTRODUCTION'] = response.xpath("//AbstractText [@Label = 'INTRODUCTION']/text()").get()
                result_abstract['METHODS'] = response.xpath("//AbstractText [@Label = 'METHODS']/text()").get()
                result_abstract['RESULTS'] = response.xpath("//AbstractText [@Label = 'RESULTS']/text()").get()
                result_abstract['DISCUSSION'] = response.xpath("//AbstractText [@Label = 'DISCUSSION']/text()").get()
                result_abstract['CONCLUSIONS'] = response.xpath("//AbstractText [@Label = 'CONCLUSIONS' or @Label = 'CONCLUSION']/text()").get()
                result_abstract['TRIAL_REGISTRATION_NUMBER'] = response.xpath("//AbstractText [@Label = 'TRIAL REGISTRATION NUMBER']/text()").get()
                result_abstract['ABSTRACTTEXT'] = response.xpath("//AbstractText/text()").get()
                result_abstract['PMID'] = response.xpath("//PMID/text()").get()
                result_abstract['ISSN'] = response.xpath("//ISSN/text()").get()
                result_abstract['pubmed'] = response.xpath("//PMID/text()").get()
                result_abstract['doi'] = response.xpath("//Article//ELocationID/text()").get()
                result_abstract['ArticleTitle'] = response.xpath("//Article//ArticleTitle/text()").get()
                result_abstract['PubDate'] = response.xpath("//PubDate//text()").get()
                result_abstract['CoiStatement'] = response.xpath("//CoiStatement/text()").get()
                result_abstract['KeywordList'] = response.xpath("//KeywordList/Keyword/text()").get()
                yield result_abstract