import os
from query_list import query_list
import pandas as pd
from wos import WosClient
import wos.utils
import suds
from bs4 import BeautifulSoup
import time
import suds
import logging
logging.getLogger('suds.client').setLevel(logging.CRITICAL)


def new_wos_query(df, counter, new_item, client, sleeper=30):
    """ratelimit this to 5 calls per 300 seconds"""
    isthisworking = 'No'
    while isthisworking == 'No':
        try:
            search = wos.utils.query(client, new_item, count=5000)
            y=BeautifulSoup(search)
            for rec in y.findAll("rec"):
                wosid = rec.uid.string
                df.at[counter, 'WOSID'] = wosid
                df.at[counter, 'Query'] = new_item
                df.at[counter, 'Date'] = rec.summary.pub_info['sortdate']
                for title in rec.summary.findAll('title'):
                    if 'type="item"' in str(title):
                        df.at[counter, 'Title'] = title.string
                        if 'type="source"' in str(title):
                            df.at[counter, 'Journal'] = title.string
                        for ident in rec.findAll('identifier'):
                            if 'type="doi"' in str(ident):
                                df.at[counter, 'DOI'] = ident['value']
                                break
                        for abstract in rec.findAll('abstract'):
                            abst = str(abstract.findAll('p'))
                            abst = abst.replace('<p>', '').replace('</p>', '')
                            abst = abst.replace('[', '').replace(']', '')
                            abst = abst.replace("\\", '')
                            df.at[counter, 'Abstract'] = abst
                        authorset = set()
                        for name in rec.findAll('name'):
                            if str(name['role'])=='author':
                                authorset.add(name.findAll('full_name')[0].string)
                                df.at[counter, 'AuthorList'] = authorset
                        if rec.keywords is not None:
                            keywordlist = ''
                            for key in rec.keywords:
                                if '<keyword>' in str(key):
                                    keywordlist = keywordlist + key.string + ';'
                                df.at[counter, 'Keywords'] = keywordlist[:-1]
                counter = counter + 1
            isthisworking = 'Yes'
        except suds.WebFault:
            isthisworking = 'No'
            time.sleep(sleeper)
    time.sleep(sleeper)
    return df, counter


def wos_main(querylist, d_path):
    """ still need to incorporate: article types, subject areas, countries"""
    columns = ['Query', 'WOSID', 'Title', 'DOI', 'AuthorList',
               'Journal', 'Date', 'Keywords',  'Abstract']
    df = pd.DataFrame(columns=columns)
    counter = 0
    with WosClient() as client:
        for item in query_list:
            new_item = " AND ".join(['TS=' + x.replace('{', '"').
                                     replace('}','"') for x in item.split(' AND ')])
            print('Searching WOS: ' + new_item + '. DF length: ', counter)
            df, counter = new_wos_query(df, counter, new_item, client)
    df.to_csv(os.path.join(d_path, 'wos', 'wos_search.csv'))
    return df
