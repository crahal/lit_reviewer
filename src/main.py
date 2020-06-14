import os
from query_list import query_list
import pandas as pd
from query_list import flaglist
from pubmed_functions import pubmed_main
from scopus_functions import scopus_main
from wos_functions import wos_main

def load_token(token_name):
    """Read supplementary text file."""
    try:
        with open(token_name, 'r') as file:
            return str(file.readline()).strip()
    except EnvironmentError:
        print('Error loading access token from file')


def build_flags(df, flaglist):
    """Build flags into the dataframe."""
    raw_len = len(df)
    for flag in flaglist:
        df[flag + '_flag'] = 0
        for index, row in df.iterrows():
            temp_df = df[df['DOI'] == row['DOI']]
            if temp_df['Query'].str.contains(flag).any():
                df.loc[index, flag + '_flag'] = 1
    df = df.sort_values(by="Abstract", na_position='last')
    df = df.drop_duplicates(subset=['DOI'], keep='first')
    print('We drop ' + str(raw_len - len(df)) + ' duplicates!')
    print('There are ' + str(len(df)) + ' papers remaining!')
    return df


if __name__ == '__main__':
    email = load_token(os.path.join(os.getcwd(), '..', 'keys', 'email_address'))
    apikey = load_token(os.path.join(os.getcwd(), '..', 'keys', 'elsevier_apikey'))
    d_path = os.path.abspath(os.path.join(os.getcwd(), '..', 'data'))
    wos_df = wos_main(query_list, d_path)
    pubmed_df = pubmed_main(d_path, query_list, email)
    scopus_df = scopus_main(apikey, d_path, query_list)
    scopus_fields = ['Query', 'Title', 'DOI', 'AuthorList', 'Journal',
                     'Date', 'Keywords', 'affilcountries', 'openaccessFlag',
                     'Abstract', 'citedbycount']
    pubmed_fields = ['Query', 'Title', 'DOI', 'AuthorList', 'Journal',
                     'Date', 'Keywords', 'Abstract',
                     'citedbycount', 'PubMedID']
    df = pd.concat([pubmed_df[pubmed_fields],
                    scopus_df[scopus_fields],
                    wos_df])
    df = build_flags(df, flaglist)
    df['in_scopus'] = pd.Series(df.DOI.isin(scopus_df.DOI).values.astype(int))
    df['in_pubmed'] = pd.Series(df.DOI.isin(pubmed_df.DOI).values.astype(int))
    df['in_wos'] = pd.Series(df.DOI.isin(wos_df.DOI).values.astype(int))
    df.to_csv(os.path.join(d_path, 'merged', 'merged_output.csv'))
