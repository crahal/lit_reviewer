import os
from query_list import query_list
import pandas as pd
from wos import WosClient
import wos.utils
import suds
from bs4 import BeautifulSoup
from datetime import datetime
from ratelimit import limits
import time
import logging
logging.getLogger('suds.client').setLevel(logging.CRITICAL)


@limits(calls=5, period=300)
def new_wos_query(new_item, client, d_path, sleeper=60):
    """ Make a Web of Science query """
    columns = ['Query', 'WOSID', 'Title', 'DOI', 'AuthorList',
               'Journal', 'Date', 'Keywords',  'Abstract']
    df = pd.DataFrame(columns=columns)
    new_item = str(new_item).replace('"','').replace('_','')
    path = os.path.join(d_path, 'wos', 'wos_ ' + new_item + '.csv')
    if os.path.exists(path) is False:
        isthisworking = 'No'
        counter = 0
        while isthisworking == 'No':
            try:
                search = wos.utils.query(client, new_item, count=5000)
                y = BeautifulSoup(search, features="lxml")
                for rec in y.findAll("rec"):
                    wosid = rec.uid.string
                    df.at[counter, 'WOSID'] = wosid
                    df.at[counter, 'Query'] = new_item
                    df.at[counter, 'Date'] = rec.summary.pub_info['sortdate']
                    for title in rec.summary.findAll('title'):
                        if 'type="item"' in str(title):
                            df.at[counter, 'Title'] = title.string
                            print(title.string)
                        if 'type="source"' in str(title):
                            df.at[counter, 'Journal'] = title.string
                            print(title.string)
                            for ident in rec.findAll('identifier'):
                                if 'type="doi"' in str(ident):
                                    df.at[counter, 'DOI'] = ident['value']
                                    break
                            for abstract in rec.findAll('abstract'):
                                abst = str(abstract.findAll('p'))
                                abst = abst.replace('<p>', '')
                                abst = abst.replace('</p>', '')
                                abst = abst.replace('[', '').replace(']', '')
                                abst = abst.replace("\\", '')
                                df.at[counter, 'Abstract'] = abst
                            auths = set()
                            for name in rec.findAll('name'):
                                if str(name['role']) == 'author':
                                    nam = name.findAll('full_name')[0].string
                                    auths.add(nam)
                                    df.at[counter, 'AuthorList'] = auths
                            if rec.keywords is not None:
                                keywords = ''
                                for key in rec.keywords:
                                    if '<keyword>' in str(key):
                                        keywords = keywords + key.string + ';'
                                    df.at[counter, 'Keywords'] = keywords[:-1]
                    counter = counter + 1
                isthisworking = 'Yes'
            except suds.WebFault:
                isthisworking = 'No'
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                print(dt_string + ': subswebfault, taking a nap...')
                time.sleep(sleeper)
        time.sleep(sleeper)
        df.to_csv(path)
        return df


def merge_dfs(d_path):
    """Merge the files from the different search terms"""
    list_of_df = []
    for subdir, dirs, files in os.walk(os.path.join(d_path, 'wos')):
        for file in files:
            if file != 'wos_search.csv':
                list_of_df.append(pd.read_csv(
                                  os.path.join(d_path, 'wos', file)))
    merged_df = pd.concat(list_of_df)
    return merged_df


def wos_main(querylist, d_path):
    ''' main function for calling web of science returns '''
    # @TODO incorporate: article types, subject areas, countries.
    with WosClient() as client:
        for item in query_list:
            # this reformats the string from query_list to make it amenable
            new_item = " AND ".join(['TS=' + x.replace('{', '"').
                                     replace('}', '"')
                                     for x in item.split(' AND ')])
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            print(dt_string + ': searching WOS: ' + new_item)
            new_wos_query(new_item, client, d_path)
            time.sleep(60)
    df = merge_dfs(d_path)
    df.to_csv(os.path.join(d_path, 'wos', 'wos_search.csv'), index=False)
    return df
