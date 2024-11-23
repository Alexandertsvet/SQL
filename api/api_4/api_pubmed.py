import requests
import lxml
from fake_useragent import UserAgent
from urllib.parse import urlparse
from lxml import html
from lxml import etree
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from io import StringIO, BytesIO
import tracemalloc
from memory_profiler import profile
import psycopg2
from dateutil import parser
from time import sleep
from tqdm import tqdm
load_dotenv()

from function import (create_search_param,
                      extract_id_set,
                      extract_from_abstract,
                      extract_identification,
                      extract_atribut_article,
                      extract_element,
                      extract_element_time,
)

dbname = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('DB_HOST')
db_post = os.getenv('DB_PORT')
try:
    # пытаемся подключиться к базе данных
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=db_host, port=db_post)
except:
    # в случае сбоя подключения будет выведено сообщение в STDOUT
    print('Can`t establish connection to database')

cursor = conn.cursor()
conn.autocommit = True
sql = '''
CREATE TABLE IF NOT EXISTS ARTICLE(
PMID VARCHAR(50) PRIMARY KEY,
DOI VARCHAR(50),
PubDate TIMESTAMP
)'''
cursor.execute(sql)
conn.commit()

cursor = conn.cursor()
conn.autocommit = True
sql = '''
CREATE TABLE IF NOT EXISTS ABSTRACT(
PMID VARCHAR(50) REFERENCES ARTICLE(PMID),
OBJECTIVES TEXT,
BACKGROUND TEXT,
INTRODUCTION TEXT,
METHODS TEXT,
RESULTS TEXT,
DISCUSSION TEXT,
CONCLUSIONS TEXT,
TRIAL_REGISTRATION_NUMBER TEXT,
ABSTRACTTEXT TEXT
)'''
cursor.execute(sql)
conn.commit()



api_key = os.getenv('api_key')
base = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
query = 'stroke[mesh]+AND+actilyse[mesh]+AND+2023:2024[pdat]'
db = 'pubmed'
api_key
url = base + f'esearch.fcgi?db=pubmed&term={query}&usehistory=y&api_key={api_key}'
user = UserAgent()
headers = {
    "User-Agent": user.chrome,
}
params = {}
session = requests.Session()
request = requests.Request('GET',url)
prepped = session.prepare_request(request)
prepped.headers=headers
response = session.send(
    prepped,
    #stream=stream,
    #verify=verify,
    #proxies=proxies,
    #cert=cert,
    #timeout=timeout
)
print(response.raise_for_status())
print(response)
print(response.headers['X-RateLimit-Limit'])
print(response.headers['X-RateLimit-Remaining'])
parser_etree = etree.XMLParser()


ids = extract_id_set(response, session, url, params, headers)
for id in tqdm(ids):
    print(id)
    retmode = 'xml'
    url_id = f"{base}efetch.fcgi?db={db}&id={id}&retmode={retmode}&api_key={api_key}"
    if int(response.headers['X-RateLimit-Remaining'])>1:
        response = session.get(url=url_id, params=params, headers=headers)
    else:
        sleep(1)
        response = session.get(url=url_id, params=params, headers=headers)
    root = etree.XML(response.content, parser_etree)

    article_id = extract_identification(root)
    abstract_articl = extract_from_abstract(root)

    date = parser.parse(",".join(extract_element_time(article_id.get('PubDate'))))
    result_article = (
        extract_element(article_id.get('PMID')),
        extract_element(article_id.get('doi')),
        f'{date}')
    sql = f"INSERT INTO ARTICLE (PMID, DOI, PubDate) VALUES{result_article}"
    cursor.execute(sql)
    conn.commit()

    PMID = extract_element(article_id.get('PMID'))
    OBJECTIVES = extract_element(abstract_articl.get('OBJECTIVES')).replace("'","''")
    BACKGROUND = extract_element(abstract_articl.get('BACKGROUND')).replace("'","''")
    INTRODUCTION = extract_element(abstract_articl.get('INTRODUCTION')).replace("'","''")
    METHODS = extract_element(abstract_articl.get('METHODS')).replace("'","''")
    RESULTS = extract_element(abstract_articl.get('RESULTS')).replace("'","''")
    DISCUSSION = extract_element(abstract_articl.get('DISCUSSION')).replace("'","''")
    CONCLUSIONS = extract_element(abstract_articl.get('CONCLUSIONS')).replace("'","''")
    TRIAL_REGISTRATION_NUMBER = extract_element(abstract_articl.get('TRIAL_REGISTRATION_NUMBER')).replace("'","''")
    ABSTRACTTEXT = extract_element(abstract_articl.get('ABSTRACTTEXT')).replace("'","''")

    sql = f"""INSERT INTO ABSTRACT (PMID, OBJECTIVES, BACKGROUND, INTRODUCTION, METHODS, RESULTS, DISCUSSION, CONCLUSIONS,TRIAL_REGISTRATION_NUMBER,ABSTRACTTEXT)\
    VALUES(\
    {PMID},\
    quote_literal('{OBJECTIVES}'),\
    quote_literal('{BACKGROUND}'),\
    quote_literal('{INTRODUCTION}'),\
    quote_literal('{METHODS}'),\
    quote_literal('{RESULTS}'),\
    quote_literal('{DISCUSSION}'),\
    quote_literal('{CONCLUSIONS}'),\
    quote_literal('{TRIAL_REGISTRATION_NUMBER}'),\
    quote_literal('{ABSTRACTTEXT}')\
    )"""
    cursor.execute(sql)
    conn.commit()


cursor.close()
conn.close()

