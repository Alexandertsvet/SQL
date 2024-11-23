from lxml import etree

def create_search_param(root):
    result = {
    'Count': int(root.xpath('//Count/text()')[0]),
    'RetMax': int(root.xpath('//RetMax/text()')[0]),
    'RetStart': int(root.xpath('//RetStart/text()')[0]),
    'QueryKey': int(root.xpath('//QueryKey/text()')[0]),
    'WebEnv': root.xpath('//WebEnv/text()')[0],
    }
    return result

def extract_id_set(response, session, url, params, headers):
    parser_etree = etree.XMLParser()
    root = etree.XML(response.content, parser_etree)
    result = create_search_param(root)
    retmax=result['Count']
    url = f'{url}&retmax={retmax}'

    if int(response.headers['X-RateLimit-Remaining'])>1:
        response = session.get(url=url, params=params, headers=headers)
    else:
        sleep(1)
        response = session.get(url=url, params=params, headers=headers)

    root = etree.XML(response.content, parser_etree)
    id = set()
    for i in root.xpath('//IdList')[0]:
        id.add(int(i.text))
    return id

def extract_from_abstract(root):
    result_abstract = {}
    result_abstract['OBJECTIVES'] = root.xpath("//AbstractText [@Label = 'OBJECTIVES' or @Label = 'OBJECTIVE']/text()")
    result_abstract['BACKGROUND'] = root.xpath("//AbstractText [@Label = 'BACKGROUND']/text()")
    result_abstract['INTRODUCTION'] = root.xpath("//AbstractText [@Label = 'INTRODUCTION']/text()")
    result_abstract['METHODS'] = root.xpath("//AbstractText [@Label = 'METHODS']/text()")
    result_abstract['RESULTS'] = root.xpath("//AbstractText [@Label = 'RESULTS']/text()")
    result_abstract['DISCUSSION'] = root.xpath("//AbstractText [@Label = 'DISCUSSION']/text()")
    result_abstract['CONCLUSIONS'] = root.xpath("//AbstractText [@Label = 'CONCLUSIONS' or @Label = 'CONCLUSION']/text()")
    result_abstract['TRIAL_REGISTRATION_NUMBER'] = root.xpath("//AbstractText [@Label = 'TRIAL REGISTRATION NUMBER']/text()")
    result_abstract['ABSTRACTTEXT'] = root.xpath("//AbstractText/text()")
    return result_abstract

def extract_identification(root):
    result_identification = {}
    result_identification['PMID'] = root.xpath("//PMID/text()")
    result_identification['ISSN'] = root.xpath("//ISSN/text()")
    result_identification['pubmed'] = root.xpath("//PMID/text()")
    result_identification['doi'] = root.xpath("//Article//ELocationID/text()")
    result_identification['ArticleTitle'] = root.xpath("//Article//ArticleTitle/text()")
    result_identification['PubDate'] = root.xpath("//PubDate//text()")
    return result_identification


def extract_atribut_article(root):
    result_atribun = {}
    result_atribun['CoiStatement'] = root.xpath("//CoiStatement/text()")
    result_atribun['KeywordList'] = root.xpath("//KeywordList/Keyword/text()")
    return result_atribun


def extract_element(element):
    if not element:
        return 'None'
    else:
        return element[0]


def extract_element_time(element):
    if not element:
        return 'None'
    else:
        if len(element)>1:
            if len(element[1])>4:
                return [element[0],element[1].split('-')[0]]
            return element
        return element