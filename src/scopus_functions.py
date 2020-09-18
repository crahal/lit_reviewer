import requests
import json
import traceback
import math
import os
import csv
from tqdm import tqdm
from ratelimit import limits
import pandas as pd
from query_list import query_list
import numpy as np


@limits(calls=3, period=1)
def call_scopus_search_api(url, apikey):
    ''' simple function for search api with ratelimited decorator'''
    api_return = requests.get(url+'&apiKey='+apikey)
    return api_return


@limits(calls=4, period=1)
def call_scopus_abstract_api(url, apikey):
    ''' simple function for abstract api with ratelimited decorator'''
    api_return = requests.get(url,
                              headers={'Accept': 'application/json',
                                       'X-ELS-APIKey': apikey})
    return api_return


@limits(calls=3, period=1)
def call_scopus_serialtitle_api(url, apikey):
    ''' simple function for title api with ratelimited  decorator'''
    api_return = requests.get(url,
                              headers={'Accept': 'application/json',
                                       'X-ELS-APIKey': apikey})
    return api_return


def search_scopus_into_csv(query_list, apikey, d_path, count=100):
    '''
    A function to scrape the scopus api for all mentions of a keyword set

    query list: a colon delimited list in the form of dataset:searchquery
    aqikey:     a free apikey granted on application: note you need to be
                connected to the domain of a registered institution or
                your requests will not be successful...
    count:      the number of returns per page request, default 100 (max)
    '''

    print('****** Now Scraping the Scopus Search API for Mentions ******')

    base_url = 'https://api.elsevier.com/content/search/scopus?'

    with open(os.path.abspath(
              os.path.join(d_path,
                           'scopus', 'search', 'parsed',
                           'scopus_search_meta.tsv')),
              'w', encoding='utf-8') as tsvfile:
        scopus_search = csv.writer(tsvfile, delimiter='\t',
                                   lineterminator='\n')
        scopus_search.writerow(['searchterm',
                                'Title',
                                'prismurl',
                                'dcidentifier',
                                'eid',
                                'dccreator',
                                'Journal',
                                'prismissn',
                                'prismeissn',
                                'prismvolume',
                                'prismissueidentifier',
                                'prismpagerange',
                                'prismcoverdate',
                                'Date',
                                'DOI',
                                'citedbycount',
                                'affilnames',
                                'affilcities',
                                'affilcountries',
                                'prismaggregationtype',
                                'subtype',
                                'subtypedescription',
                                'sourceid',
                                'openaccess',
                                'openaccessFlag'])
        for search in query_list:
            pagenum = 0
            numres = 99999999999999999999999999999999999999999999999999999999
            while pagenum < (math.ceil(int(numres)/count)):
                search = search.lower()
                search = search.replace('"', '%22')
                search = search.replace(' ', '%20')
                search = search.replace('{', '%22')
                search = search.replace('}', '%22')
                search = search.replace('&', '+AND+')
                url = base_url + 'start=' + str((pagenum*count)) + '&count=' + \
                      str(count) +'&query=TITLE-ABS-KEY('+ search+\
                      ')%20%20AND%20%20PUBYEAR%20%20%3E%20%202017%20'
                      #this last term sets the year range
                api_return = call_scopus_search_api(url, apikey)
                api_json = json.loads(api_return.content)
                try:
                    remaining = api_return.headers['X-RateLimit-Remaining']
                    print('We have: ' + str(remaining) + ' calls remaining')
                    if pagenum == 0:
                        numres = api_json['search-results']['opensearch:totalResults']
                        print('There are a total of ' + numres +
                              ' returns for ' + search)
                except:
                    pass
                pagenum += 1
                try:
                    for entry in api_json['search-results']['entry']:
                        try:
                            Title = entry['dc:title']
                        except KeyError:
                            Title = 'N/A'
                        try:
                            prismurl = entry['prism:url']
                        except KeyError:
                            prismurl = 'N/A'
                        try:
                            dcidentifier = entry['dc:identifier']
                        except KeyError:
                            dcidentifier = 'N/A'
                        try:
                            eid = entry['eid']
                        except KeyError:
                            eid = 'N/A'
                        try:
                            dccreator = entry['dc:creator']
                        except KeyError:
                            dccreator = 'N/A'
                        try:
                            prismpublicationname = entry['prism:publicationName']
                        except KeyError:
                            prismpublicationname = 'N/A'
                        try:
                            prismissn = entry['prism:issn']
                        except KeyError:
                            prismissn = 'N/A'
                        try:
                            prismeissn = entry['prism:eIssn']
                        except KeyError:
                            prismeissn = 'N/A'
                        try:
                            prismvolume = entry['prism:volume']
                        except KeyError:
                            prismvolume = 'N/A'
                        try:
                            prismissueidentifier = entry['prism:issueIdentifier']
                        except KeyError:
                            prismissueidentifier = 'N/A'
                        try:
                            prismpagerange = entry['prism:pageRange']
                        except KeyError:
                            prismpagerange = 'N/A'
                        try:
                            prismcoverdate = entry['prism:coverDate']
                        except KeyError:
                            prismcoverdate = 'N/A'
                        try:
                            prismcoverdisplaydate = entry['prism:coverDisplayDate']
                        except KeyError:
                            prismcoverdisplaydate = 'N/A'
                        try:
                            DOI = entry['prism:doi']
                        except KeyError:
                            DOI = 'N/A'
                        try:
                            citedbycount = entry['citedby-count']
                        except KeyError:
                            citedbycount = 'N/A'
                        try:
                            affilnames = ''
                            affilcities = ''
                            affilcountries = ''
                            for affiliation in entry['affiliation']:
                                affilnames = affilnames + str(affiliation['affilname']) + ':'
                                affilcities = affilcities + str(affiliation['affiliation-city']) + ':'
                                affilcountries = affilcountries + str(affiliation['affiliation-country']) + ':'
                            affilnames = affilnames.replace('None:', '')[:-1]
                            affilcities = affilcities.replace('None:', '')[:-1]
                            affilcountries = affilcountries.replace('None:', '')[:-1]
                        except KeyError:
                            affilnames = 'N/A'
                            affilcities = 'N/A'
                            affilcountries = 'N/A'
                        try:
                            prismaggregationtype = entry['prism:aggregationType']
                        except KeyError:
                            prismaggregationtype = 'N/A'
                        try:
                            subtype = entry['subtype']
                        except KeyError:
                            subtype = 'N/A'
                        try:
                            subtypedescription = entry['subtypeDescription']
                        except KeyError:
                            subtypedescription = 'N/A'
                        try:
                            sourceid = entry['source-id']
                        except KeyError:
                            sourceid = 'N/A'
                        try:
                            openaccess = entry['openaccess']
                        except KeyError:
                            openaccess = 'N/A'
                        try:
                            openaccessflag = entry['openaccessFlag']
                        except KeyError:
                            openaccessflag = 'N/A'
                        scopus_search.writerow([search,
                                                Title,
                                                prismurl,
                                                dcidentifier,
                                                eid,
                                                dccreator,
                                                prismpublicationname,
                                                prismissn,
                                                prismeissn,
                                                prismvolume,
                                                prismissueidentifier,
                                                prismpagerange,
                                                prismcoverdate,
                                                prismcoverdisplaydate,
                                                DOI,
                                                citedbycount,
                                                affilnames,
                                                affilcities,
                                                affilcountries,
                                                prismaggregationtype,
                                                subtype,
                                                subtypedescription,
                                                sourceid,
                                                openaccess,
                                                openaccessflag])
                except Exception as e:
                    print('No entries in the json return for ' +
                          str(search.title().replace('%22',
                                                    '').replace('%20',
                                                                ' ')))
                if api_return.headers['X-RateLimit-Remaining'] == 0:
                    print('No limit remaining!')
                    break
                with open(os.path.abspath(
                          os.path.join(d_path, 'scopus', 'search', 'raw',
                                       search.replace(' ','_').replace('"','') +
                                       '_' + str(pagenum+1) + '.json')),
                          'w') as outfile:
                    json.dump(api_json, outfile)


def scopus_abstract(doi_list, apikey, d_path, count=25):
    with open(os.path.abspath(
              os.path.join(d_path, 'scopus', 'abstract_info', 'parsed',
                           'abstract_info.tsv')),
              'w', encoding='utf-8') as tsvfile:
        abstract_info = csv.writer(tsvfile, delimiter='\t',
                                   lineterminator='\n')
        abstract_info.writerow(['DOI', 'AuthorList', 'authoridlist',
                                'dctitle', 'Abstract', 'correspondanceperson',
                                'Keywords', 'datecreated',
                                'number_references'])
        base_url = 'https://api.elsevier.com/content/abstract/doi/'
        for doi in tqdm(doi_list):
            url = base_url + str(doi)
            api_return = call_scopus_abstract_api(url, apikey)
            try:
                remaining = api_return.headers['X-RateLimit-Remaining']
                if int(remaining) % 10 == 0:
                    print('Remaining calls:' + remaining)
            except KeyError as e:
                print(e)
            if api_return.status_code==200:
                api_text = json.loads(api_return.text)
                try:
                    author_list = ''
                    authorid_list = ''
                    if type(api_text['abstracts-retrieval-response']\
                                    ['item']['bibrecord']['head']\
                                    ['author-group']) is list:
                        for author in api_text['abstracts-retrieval-response']\
                                              ['item']['bibrecord']['head']\
                                              ['author-group']:
                            for nextauthor in author['author']:
                                author_list = author_list + \
                                              nextauthor['preferred-name']\
                                                        ['ce:given-name']
                                author_list = author_list + ' ' +\
                                              nextauthor['preferred-name']\
                                                        ['ce:surname'] + '; '
                                try:
                                    authorid_list = authorid_list +\
                                                    nextauthor['@auid'] + '; '
                                except:
                                    authorid_list = 'N/A'
                    elif type(api_text['abstracts-retrieval-response']\
                                      ['item']['bibrecord']['head']\
                                      ['author-group']) is dict:
                        for author in api_text['abstracts-retrieval-response']\
                                              ['item']['bibrecord']['head']\
                                              ['author-group']['author']:
                            author_list = author_list + \
                                          author['preferred-name']\
                                                ['ce:given-name']
                            author_list = author_list + ' ' +\
                                          author['preferred-name']\
                                                ['ce:surname'] + '; '
                            try:
                                authorid_list = authorid_list +\
                                                author['@auid'] + '; '
                            except:
                                authorid_list = 'N/A'
                    else:
                        print('not even getting here')
                except (TypeError, KeyError):
                    print(traceback.format_exc())
                    authorname = 'N/A'
                try:
                    dctitle = api_text['abstracts-retrieval-response']\
                                       ['item']['bibrecord']['head']\
                                       ['citation-title']
                except:
                    dctitle = 'N/A'
                try:
                    abstracts = api_text['abstracts-retrieval-response']\
                                        ['item']['bibrecord']['head']\
                                        ['abstracts']
                except:
                    abstracts = 'N/A'
                try:

                    Keywords = ''
                    for keyword in api_text['abstracts-retrieval-response']\
                                           ['authkeywords']['author-keyword']:
                        if len(keyword)==0:
                            Keywords = 'N/A'
                        else:
                            Keywords = Keywords + keyword['$'] + '; '
                    Keywords = Keywords[:-2]
                except:
                    Keywords = 'N/A'
                try:
                    person = api_text['abstracts-retrieval-response']['item']\
                                     ['bibrecord']['head']['correspondence']\
                                     ['person']
                    correspondance = person['ce:given-name']
                    correspondance = correspondance + person['ce:surname']
                except:
                    correspondance = 'N/A'
                try:
                    datecreated = api_text['abstracts-retrieval-response']\
                                          ['item']['bibrecord']['head']\
                                          ['source']['publicationdate']\
                                          ['date-text']['$']
                except:
                    datecreated = 'N/A'
                try:
                    number_references = api_text['abstracts-retrieval-response']\
                                                ['item']['bibrecord']['tail']\
                                                ['bibliography']['@refcount']
                except:
                    number_references = 'N/A'
                abstract_info.writerow([doi,
                                        author_list[:-2],
                                        authorid_list[:-2],
                                        dctitle,
                                        abstracts,
                                        correspondance,
                                        Keywords,
                                        datecreated,
                                        number_references])
            if int(remaining) == 0:
                print('out of calls! doh! gotta wait a while, I guess...')
                break


def scopus_serial_title(journal_title_list, apikey, d_path):
    '''
        not all of these generate 200s, some generate 404s, so do a check on
        the response before looping over entries...
    '''

    print('************ Now Scraping Scopus for Journal Meta ***********')

    with open(os.path.abspath(
              os.path.join(d_path,
                           'scopus', 'serialtitle', 'parsed',
                           'serial_title.tsv')),
              'w', encoding='utf-8') as tsvfile:
        serial_title = csv.writer(tsvfile, delimiter='\t',
                                  lineterminator='\n')
        serial_title.writerow(['prismissn',
                               'dctitle',
                               'dcpublisher',
                               'prismaggregationtype',
                               'sourceid',
                               'prismeissn',
                               'openaccess',
                               'openaccessarticle',
                               'openarchivearticle',
                               'openaccesstype',
                               'openaccessstartdate',
                               'oaallowsauthorpaid',
                               'subjectarea',
                               'subjectcodes',
                               'subjectabvs',
                               'subjectvalues',
                               'sniplist',
                               'snipfa',
                               'snipyear',
                               'snipscore',
                               'sjrlist',
                               'sjrfa',
                               'sjryear',
                               'sjrscore',
                               'citescoreyearinfolist',
                               'citescorecurrentmetric',
                               'citescorecurrentmetricyear',
                               'citescoretracker',
                               'citescoretrackeryear'])
        base_url = 'https://api.elsevier.com/content/serial/title/issn/'
        for issn in tqdm(journal_title_list):
            url = base_url + str(issn)
            api_return = call_scopus_serialtitle_api(url, apikey)
            if api_return.status_code != 404:
                api_json = json.loads(api_return.text)
                for entry in api_json['serial-metadata-response']['entry']:
                    try:
                        prismissn = issn
                    except KeyError:
                        prismissn = 'N/A'
                    try:
                        dctitle = entry['dc:title']
                    except KeyError:
                        dctitle = 'N/A'
                    try:
                        dcpublisher = entry['dc:publisher']
                    except KeyError:
                        dcpublisher = 'N/A'
                    try:
                        prismaggregationtype = entry['prism:aggregationType']
                    except KeyError:
                        prismaggregationtype = 'N/A'
                    try:
                        sourceid = entry['source-id']
                    except KeyError:
                        sourceid = 'N/A'
                    try:
                        prismeissn = entry['prism:eIssn']
                    except KeyError:
                        prismeissn = 'N/A'
                    try:
                        if entry['openaccess'] is not None:
                            openaccess = entry['openaccess']
                        else:
                            openaccess = 'N/A'
                    except KeyError:
                        openaccess = 'N/A'
                    try:
                        if entry['openaccessArticle'] is not None:
                            openaccessarticle = entry['openaccessArticle']
                        else:
                            openaccessarticle = 'N/A'
                    except KeyError:
                        openaccessarticle = 'N/A'
                    try:
                        if entry['openArchiveArticle'] is not None:
                            openarchivearticle = entry['openArchiveArticle']
                        else:
                            openarchivearticle = 'N/A'
                    except KeyError:
                        openarchivearticle = 'N/A'
                    try:
                        if entry['openaccessType'] is not None:
                            openaccesstype = entry['openaccessType']
                        else:
                            openaccesstype = 'N/A'
                    except KeyError:
                        openaccesstype = 'N/A'
                    try:
                        if entry['openaccessStartDate'] is not None:
                            openaccessstartdate = entry['openaccessStartDate']
                        else:
                            openaccessstartdate = 'N/A'
                    except KeyError:
                        openaccessstartdate = 'N/A'
                    try:
                        if entry['oaAllowsAuthorPaid'] is not None:
                            oaallowsauthorpaid = entry['oaAllowsAuthorPaid']
                        else:
                            oaallowsauthorpaid = 'N/A'
                    except KeyError:
                        oaallowsauthorpaid = 'N/A'
                    try:
                        subjectarea = entry['subject-area']
                        subjectcodes = ''
                        subjectabvs = ''
                        subjectvalues = ''
                        for subject in subjectarea:
                            subjectcodes = subjectcodes + subject['@code'] + ':'
                            subjectabvs = subjectabvs + subject['@abbrev'] + ':'
                            subjectvalues = subjectvalues + subject['$'] + ':'
                        subjectcodes = subjectcodes[:-1]
                        subjectabvs = subjectabvs[:-1]
                        subjectvalues = subjectvalues[:-1]
                    except KeyError:
                        subjectarea = 'N/A'
                        subjectcodes = 'N/A'
                        subjectabvs = 'N/A'
                        subjectvalues = 'N/A'
                    except KeyError:
                        subjectarea = 'N/A'
                    try:
                        sniplist = entry['SNIPList']
                    except KeyError:
                        sniplist = 'N/A'
                    try:
                        snipfa = entry['SNIPList']['SNIP'][0]['@_fa']
                    except KeyError:
                        snipfa = 'N/A'
                    try:
                        snipyear = entry['SNIPList']['SNIP'][0]['@year']
                    except KeyError:
                        snipyear = 'N/A'
                    try:
                        snipscore = entry['SNIPList']['SNIP'][0]['$']
                    except KeyError:
                        snipscore = 'N/A'
                    try:
                        sjrfa = entry['SJRList']['SJR'][0]['@_fa']
                    except KeyError:
                        sjrfa = 'N/A'
                    try:
                        sjrpyear = entry['SJRList']['SJR'][0]['@year']
                    except KeyError:
                        sjrpyear = 'N/A'
                    try:
                        sjrpscore = entry['SJRList']['SJR'][0]['$']
                    except KeyError:
                        sjrpscore = 'N/A'
                    try:
                        sjrlist = entry['SJRList']
                    except KeyError:
                        sjrlist = 'N/A'
                    try:
                        citescoreyearinfolist = entry['citeScoreYearInfoList']
                    except KeyError:
                        citescoreyearinfolist = 'N/A'
                    try:
                        citescorecurrentmetric = entry['citeScoreYearInfoList']['citeScoreCurrentMetric']
                    except KeyError:
                        citescorecurrentmetric = 'N/A'
                    try:
                        citescorecurrentmetricyear = entry['citeScoreYearInfoList']['citeScoreCurrentMetricYear']
                    except KeyError:
                        citescorecurrentmetricyear = 'N/A'
                    try:
                        citescoretracker = entry['citeScoreYearInfoList']['citeScoreTrackerYear']
                    except KeyError:
                        citescoretracker = 'N/A'
                    try:
                        citescoretrackeryear = entry['citeScoreYearInfoList']['citeScoreTrackerYear']
                    except KeyError:
                        citescoretrackeryear = 'N/A'
                    serial_title.writerow([prismissn,
                                           dctitle,
                                           dcpublisher,
                                           prismaggregationtype,
                                           sourceid,
                                           prismeissn,
                                           openaccess,
                                           openaccessarticle,
                                           openarchivearticle,
                                           openaccesstype,
                                           openaccessstartdate,
                                           oaallowsauthorpaid,
                                           subjectarea,
                                           subjectcodes,
                                           subjectabvs,
                                           subjectvalues,
                                           sniplist,
                                           snipfa,
                                           snipyear,
                                           snipscore,
                                           sjrlist,
                                           sjrfa,
                                           sjrpyear,
                                           sjrpscore,
                                           citescoreyearinfolist,
                                           citescorecurrentmetric,
                                           citescorecurrentmetricyear,
                                           citescoretracker,
                                           citescoretrackeryear])
            elif (api_return.status_code != 200) and\
                 (api_return.status_code != 404):
                print('whats going on here? api status code is: ' +
                      str(api_return.status_code))


def make_affil_df(search_df):
    ''' make a long-format file for affiliations'''

    print('*********** Now Making a Long Affiliations Dataset **********')

    search_df = search_df[search_df['affilnames'].notnull()]
    search_df = search_df[search_df['affilcities'].notnull()]
    search_df = search_df[search_df['affilcountries'].notnull()]
    citedbycount = []
    affilnames = []
    affilcities = []
    affilcountries = []
    authorname = []
    doi = []
    for indexval in search_df.index:
        if isinstance(search_df.at[indexval, 'affilnames'], str) and \
           isinstance(search_df.at[indexval, 'affilcities'], str) and \
           isinstance(search_df.at[indexval, 'affilcountries'], str):
            for affils in range(0, len(search_df.at[indexval, 'affilnames'].split(':'))):
                affilnames.append(search_df.at[indexval, 'affilnames'].split(':')[affils])
                doi.append(search_df.at[indexval, 'DOI'])
                authorname.append(search_df.at[indexval, 'dccreator'])
                citedbycount.append(search_df.at[indexval, 'citedbycount'])
                try:
                    affilcities.append(search_df.at[indexval, 'affilcities'].split(':')[affils])
                except IndexError:
                    affilcities.append('N/A')
                try:
                    affilcountries.append(search_df.at[indexval, 'affilcountries'].split(':')[affils])
                except IndexError:
                    affilcountries.append('N/A')
    affil_df = pd.DataFrame()
    affil_df['University'] = affilnames
    affil_df['Cities'] = affilcities
    affil_df['Countries'] = affilcountries
    affil_df['citedbycount'] = citedbycount
    affil_df['AuthorName'] = authorname
    affil_df['DOI'] = doi
    return affil_df


def make_keywords_df(abstract_df):
    ''' this makes a long dataframe of keywords '''
    print('**** Now making a long keywords dataset ****')
    abstract_df = abstract_df[abstract_df['DOI'].notnull()]
    abstract_df = abstract_df[abstract_df['Keywords'].notnull()]
    doilist = []
    Keywords = []
    for indexval in abstract_df.index:
        for keyword in range(0, len(abstract_df.at[indexval,
                                                   'Keywords'].split(';'))):
            doilist.append(abstract_df.at[indexval, 'DOI'])
            Keywords.append(abstract_df.at[indexval,'Keywords'].split(';')[keyword])
    keywords_df = pd.DataFrame()
    keywords_df['DOI'] = doilist
    keywords_df['keywords'] = Keywords
    return keywords_df


def make_authorlist_df(abstract_df):
    print('**** Now making a long authorlist dataset ****')
    abstract_df = abstract_df[abstract_df['DOI'].notnull()]
    abstract_df = abstract_df[abstract_df['author_list'].notnull()]
    abstract_df = abstract_df[abstract_df['authoridlist'].notnull()]
    doilist = []
    authorlist = []
    authoridlist = []
    for indexval in abstract_df.index:
        for author in range(0, len(abstract_df.at[indexval,
                                                  'author_list'].split(';'))):
            doilist.append(abstract_df.at[indexval, 'DOI'])
            authorlist.append(abstract_df.at[indexval,'author_list'].split(';')[author])
            try:
                authoridlist.append(abstract_df.at[indexval,'authoridlist'].split(';')[author])
            except:
                authoridlist.append('N/A')
    author_df = pd.DataFrame()
    author_df['DOI'] = doilist
    author_df['author_list'] = authorlist
    author_df['authoridlist'] = authoridlist
    return author_df


def make_subjects_df(merged_df):
    print('********* Now Making a Long Subjects Studied Dataset ********')
    doi = []
    subjectvals = []
    subjectabrevs = []
    citedbycount = []
    for indexval in merged_df.index:
        if isinstance(merged_df.at[indexval, 'subjectabvs'], str) and \
           isinstance(merged_df.at[indexval, 'subjectvalues'], str):
            for subjects in range(0, len(merged_df.at[indexval,
                                                      'subjectabvs'].split(':'))):
                citedbycount.append(merged_df.at[indexval,
                                                 'citedbycount'])
                doi.append(merged_df.at[indexval,
                                        'DOI'])
                subjectabrevs.append(merged_df.at[indexval,
                                     'subjectabvs'].split(':')[subjects])
                subjectvals.append(merged_df.at[indexval,
                                                'subjectvalues'].split(':')[subjects])
    subjects_df = pd.DataFrame()
    subjects_df['citedbycount'] = citedbycount
    subjects_df['subjectvalues'] = subjectvals
    subjects_df['DOI'] = doi
    subjects_df['subjectabrevs'] = subjectabrevs
    return subjects_df


def scopus_main(apikey, d_path, query_list):
    search_scopus_into_csv(query_list, apikey, d_path)
    scopus_df = pd.read_csv(os.path.join(d_path, 'scopus', 'search',
                                         'parsed', 'scopus_search_meta.tsv'),
                            index_col=None, sep='\t')
    scopus_df['prismyear'] = scopus_df['prismcoverdate'].str.split('-').str[0]
    scopus_serial_title(list(scopus_df[scopus_df['prismissn'].
                             notnull()]['prismissn'].unique()), apikey, d_path)
    scopus_df['openaccess'] = pd.to_numeric(scopus_df['openaccess'],
                                            errors='coerce')
    doi_list = scopus_df['DOI'].dropna().unique().tolist()
    scopus_abstract(doi_list, apikey, d_path)
    scopus_merged_df = merge_datasets(d_path)
    return scopus_merged_df


def merge_datasets(d_path):
    """docstring."""
    scopus_df = pd.read_csv(os.path.join(d_path, 'scopus', 'search',
                                         'parsed', 'scopus_search_meta.tsv'),
                            index_col=None, sep='\t')
    scopus_df['prismyear'] = scopus_df['prismcoverdate'].str.split('-').str[0]
    scopus_df['openaccess'] = pd.to_numeric(scopus_df['openaccess'],
                                            errors='coerce')
    serial_title_df = pd.read_csv(os.path.join(d_path, 'scopus',
                                               'serialtitle', 'parsed',
                                               'serial_title.tsv'),
                                  index_col=None, sep='\t')
    serial_title_df = serial_title_df.rename({'sourceid': 'journal_sourceid'},
                                             axis=1)
    serial_title_df = serial_title_df.rename({'dctitle': 'journal_dctitle'},
                                             axis=1)
    serial_title_df = serial_title_df.rename({'prismaggregationtype':
                                              'journal_prismaggregationtype'},
                                              axis=1)
    serial_title_df = serial_title_df.rename({'prismeissn':
                                              'journal_prismeissn'}, axis=1)
    serial_title_df = serial_title_df.rename({'openaccess':
                                              'journal_openaccess'}, axis=1)
    serial_title_df = serial_title_df.rename({'sourceid': 'journal_sourceid'},
                                             axis=1)
    scopus_merged_df = pd.merge(scopus_df, serial_title_df, how='left',
                                left_on='prismissn', right_on='prismissn')
    abstract_df = pd.read_csv(os.path.join(d_path, 'scopus', 'abstract_info',
                                           'parsed', 'abstract_info.tsv'),
                              index_col=None, sep='\t')
    abstract_df = abstract_df.rename({'dctitle': 'abstract_dctitle'}, axis=1)
    scopus_merged_df = pd.merge(scopus_merged_df, abstract_df,
                                how='left', left_on='DOI',
                                right_on='DOI')
    scopus_merged_df = scopus_merged_df.rename(columns={'searchterm': 'Query'})
    scopus_merged_df['Query'] = scopus_merged_df['Query'].str.replace('%20', " ")
    scopus_merged_df['Query'] = scopus_merged_df['Query'].str.replace('{', '"')
    scopus_merged_df['Query'] = scopus_merged_df['Query'].str.replace('}', '"')
#    scopus_merged_df = scopus_merged_df.drop_duplicates(subset=['DOI'])
    scopus_merged_df.to_csv(os.path.join(d_path, 'scopus', 'merged',
                                         'scopus_merged_cleaned.csv'))
    return scopus_merged_df
