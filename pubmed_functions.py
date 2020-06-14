import pandas as pd
import re
import os
from Bio import Entrez
import time


def search(query, email):
    Entrez.email = email
    handle = Entrez.esearch(db='pubmed', sort='relevance', retmode='xml',
                            retmax=100000, term=query)
    return Entrez.read(handle)


def return_abstract(paper):
    try:
        AbstractList = paper['MedlineCitation']['Article']['Abstract']
        Abstract = str(AbstractList['AbstractText'])
        Abstract = Abstract.replace('[', '')
        Abstract = Abstract.replace(']', '')
        if 'StringElement' in Abstract:
            abstractlist = re.findall(r'StringElement\((.*?), attributes=',
                                      str(Abstract))
            Abstract = ". ".join(abstractlist)
        Abstract = re.sub("<[^)]*>", "", Abstract)
        Abstract = Abstract.encode('ascii', 'ignore')
        Abstract = Abstract.decode().replace('\"', '')
        Abstract = Abstract.replace("\'", '')
        Abstract = Abstract.replace("\\", '')
        Abstract = Abstract.replace(r"\xa0", "")
    except KeyError:
        Abstract = 'N/A'
    return Abstract


def return_pmid(paper):
    return paper['MedlineCitation']['PMID']


def return_keywords(paper):
    try:
        keywords = ''
        for keyword in paper['MedlineCitation']['KeywordList'][0]:
            keywords = keywords + ',  ' + keyword
        keywords = keywords[2:]
    except IndexError:
        keywords = 'N/A'
    return [keywords]


def return_date(paper):
    paper = paper['MedlineCitation']['Article']
    try:
        day = paper['ArticleDate'][0]['Day']
    except IndexError:
        try:
            day = paper['Journal']['JournalIssue']['PubDate']['Day']
        except KeyError:
            day = 'N/A'
    try:
        month = paper['ArticleDate'][0]['Month']
    except IndexError:
        try:
            month = paper['Journal']['JournalIssue']['PubDate']['Month']
        except KeyError:
            month = 'N/A'
    try:
        year = paper['ArticleDate'][0]['Year']
    except IndexError:
        try:
            year = paper['Journal']['JournalIssue']['PubDate']['Year']
        except KeyError:
            year = 'N/A'
    return [str(day) + '-' + str(month) + '-' + str(year), year]


def return_title(paper):
    return paper['MedlineCitation']['Article']['ArticleTitle']


def return_journal(paper):
    return paper['MedlineCitation']['Article']['Journal']['Title']


def return_type(paper):
    return str(paper['MedlineCitation']['Article']['PublicationTypeList'][0])


def return_pag(paper):
    try:
        return paper['MedlineCitation']['Article']['Pagination']['MedlinePgn']
    except KeyError:
        return None


def return_doi(paper):
    try:
        doi = None
        for element in paper['PubmedData']['ArticleIdList']:
            if element.attributes['IdType'] == 'doi':
                doi = str(paper['PubmedData']['ArticleIdList'][1])
        return doi
    except IndexError:
        return None


def return_authors(paper):
    authorlist = []
    try:
        for author in paper['MedlineCitation']['Article']['AuthorList']:
            try:
                authorlist.append(author['LastName'] + ', ' +
                                  author['ForeName'])
            except ValueError:
                pass
    except KeyError:
        pass
    return str(authorlist)


def call_entrez(id):
    while True:
        try:
            results = Entrez.read(Entrez.elink(dbfrom='pubmed', db='pmc',
                                               LinkName='pubmed_pmc_refs',
                                               id=id))
        except IndexError:
            time.sleep(100)
            continue
        break
    return results


def return_cite(pubmedid, email):
    Entrez.email = email
    results = call_entrez(pubmedid)
    try:
        count = str(len(results[0]['LinkSetDb'][0]['Link']))
    except IndexError:
        count = 0
    return count


def pubmed_main(d_path, querylist, email):
    """Generate a dataframe from pubmed API calls."""
    df = pd.DataFrame(columns=['Title', 'DOI', 'AuthorList', 'Journal', 'Year',
                               'Date', 'Keywords', 'Type', 'Pagination',
                               'citedbycount', 'Abstract', 'Query'])
    querylist = [x.replace('{', '"').replace('}', '"') for x in querylist]
    for query in querylist:
        results = search(query, email)
        print(str(len(results['IdList'])) + ' terms for: ' + str(query))
        if len(results['IdList']) > 0:
            papers = Entrez.read(Entrez.efetch(db='pubmed', retmode='xml',
                                               id=','.join(results['IdList'])))
            for i, paper in enumerate(papers['PubmedArticle']):
                pmid = return_pmid(paper)
                if return_pmid(paper) not in df.index:
                    df = df.append(pd.Series(name=return_pmid(paper),
                                             dtype='object'))
                    df.loc[pmid, 'Abstract'] = return_abstract(paper)
                    df.loc[pmid, 'Keywords'] = return_keywords(paper)[0]
                    df.loc[pmid, 'Date'] = return_date(paper)[0]
                    df.loc[pmid, 'Year'] = return_date(paper)[1]
                    df.loc[pmid, 'Title'] = return_title(paper)
                    df.loc[pmid, 'DOI'] = return_doi(paper)
                    df.loc[pmid, 'Type'] = return_type(paper)
                    df.loc[pmid, 'citedbycount'] = return_cite(pmid, email)
                    df.loc[return_pmid(paper),
                           'Journal'] = return_journal(paper)
                    df.loc[return_pmid(paper),
                           'Query'] = query
                    df.loc[return_pmid(paper),
                           'Pagination'] = return_pag(paper)
                    df.loc[return_pmid(paper),
                           'AuthorList'] = return_authors(paper)
    df = df.reset_index().rename({'index': 'PubMedID'}, axis=1)
    df.to_csv(os.path.join(d_path, 'pubmed',
                           'pubmed_merged_cleaned.csv'))
    return df
