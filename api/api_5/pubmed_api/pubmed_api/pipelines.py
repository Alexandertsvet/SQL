# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from clickhouse_driver import Client


class PubmedApiPipeline:
    def __init__(self) -> None:
        client = Client(host='localhost',
                user='default',
                password='',
                port=9000)
        self.client = client

        try:
            result = client.execute('SHOW TABLES')
            print(result)
        except Exception as e:
            print(f"Encountered an error: {e}")

        client.execute(
        '''
        CREATE TABLE IF NOT EXISTS test
        (
            `PMID` String,
            `OBJECTIVES` String,
            `BACKGROUND` String,
            `INTRODUCTION` String,
            `METHODS` String,
            `RESULTS` String,
            `DISCUSSION` String,
            `CONCLUSIONS` String,
            `TRIAL_REGISTRATION_NUMBER` String,
            `ABSTRACTTEXT` String,
            `ISSN` String,
            `doi` String,
            `ArticleTitle` String,
            `PubDate` String,
            `CoiStatement` String,
            `KeywordList`String
        )
        ENGINE = MergeTree
        PRIMARY KEY (PMID)
        ORDER BY PMID;
        '''
        )


    def process_item(self, item, spider):
            PMID = str(item.get('PMID'))
            OBJECTIVES = str(item.get('OBJECTIVES'))
            BACKGROUND = str(item.get('BACKGROUND'))
            INTRODUCTION = str(item.get('INTRODUCTION'))
            METHODS = str(item.get('METHODS'))
            RESULTS = str(item.get('RESULTS'))
            DISCUSSION = str(item.get('DISCUSSION'))
            CONCLUSIONS = str(item.get('CONCLUSIONS'))
            TRIAL_REGISTRATION_NUMBER = str(item.get('TRIAL_REGISTRATION_NUMBER'))
            ABSTRACTTEXT = str(item.get('ABSTRACTTEXT'))
            ISSN = str(item.get('ISSN'))
            doi = str(item.get('doi'))
            ArticleTitle = str(item.get('ArticleTitle'))
            PubDate = str(item.get('PubDate'))
            CoiStatement = str(item.get('CoiStatement'))
            KeywordList = str(item.get('KeywordList'))

            sql = f'''INSERT INTO test (*) VALUES ('{PMID}',\
            '{OBJECTIVES}',\
            '{BACKGROUND}',\
            '{INTRODUCTION}',\
            '{METHODS}',\
            '{RESULTS}',\
            '{DISCUSSION}',\
            '{CONCLUSIONS}',\
            '{TRIAL_REGISTRATION_NUMBER}',\
            '{ABSTRACTTEXT}',\
            '{ISSN}',\
            '{doi}',\
            '{ArticleTitle}',\
            '{PubDate}',\
            '{CoiStatement}',\
            '{KeywordList}')\
            '''
            self.client.execute(sql)
            return item
