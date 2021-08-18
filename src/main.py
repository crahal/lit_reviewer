import os
from query_list import query_list
import pandas as pd
from query_list import flaglist
from pubmed_functions import pubmed_main
from scopus_functions import scopus_main
from wos_functions import wos_main


def main():
    """ main function for lit_reviewer"""

    email = load_token(os.path.join(os.getcwd(), '..', 'keys',
                                    'email_address'))
    apikey = load_token(os.path.join(os.getcwd(), '..',
                                     'keys', 'elsevier_apikey'))
    d_path = os.path.abspath(os.path.join(os.getcwd(), '..', 'data'))
    wos_df = wos_main(query_list, d_path)
    pubmed_df = pubmed_main(d_path, query_list, email)
    scopus_df = scopus_main(apikey, d_path, query_list)
    pubmed_df = pd.read_csv(os.path.join(d_path, 'pubmed',
                                         'pubmed_merged_cleaned.csv'))
    scopus_df = pd.read_csv(os.path.join(d_path, 'scopus',
                                         'merged', 'scopus_merged_cleaned.csv'))
    wos_df = pd.read_csv(os.path.join(d_path, 'wos', 'wos_search.csv'))

    scopus_fields = ['Query', 'Title', 'DOI', 'AuthorList', 'Journal',
                     'Date', 'Keywords', 'affilcountries', 'openaccessFlag',
                     'Abstract', 'citedbycount']
    pubmed_fields = ['Query', 'Title', 'DOI', 'AuthorList', 'Journal',
                     'Date', 'Keywords', 'Abstract', 'citedbycount', 'PubMedID']
    df = pd.concat([pubmed_df[pubmed_fields],
                    scopus_df[scopus_fields],
                    wos_df])
    df = build_flags(df, flaglist)
    df['in_scopus'] = pd.Series(df.DOI.isin(scopus_df.DOI).values.astype(int))
    df['in_pubmed'] = pd.Series(df.DOI.isin(pubmed_df.DOI).values.astype(int))
    df['in_wos'] = pd.Series(df.DOI.isin(wos_df.DOI).values.astype(int))
    df = df.sort_values(by="Abstract", na_position='last')
    df = df.drop_duplicates(subset=['DOI'], keep='first')
    df = df.drop_duplicates(subset=['Title'], keep='first')
    df.to_csv(os.path.join(d_path, 'merged', 'merged_output.csv'))


def load_token(token_name):
    """ Read supplementary text file for Scopus API key

    Args:
        token_name: the name of your Scopus API key file
    """
    try:
        with open(token_name, 'r') as file:
            return str(file.readline()).strip()
    except EnvironmentError:
        print(f'Error loading {token_name} from file')


def build_flags(df_merged, flaglist):
    """Build flags into the dataframe.

    Args:
        df_merged: the full merged dataframe
        flaglist: the list of flags from query_list
    Return:
        df with list of flags
    """
    df_merged = df_merged[df_merged['DOI'].notnull()]
    df_merged = df_merged[df_merged['DOI']!='']
    df_merged = df_merged[df_merged['Title'].notnull()]
    df_merged = df_merged[df_merged['Title']!='']
    df_merged = df_merged.reset_index(drop=True)
    for flag in flaglist:
        print('Building flags for: ' + str(flag))
        # @TODO i think this should be an .at[:,]
        df_merged[flag + '_flag'] = 0
        for index, row in df.iterrows():
            temp_df = df_merged[df_merged['DOI'] == row['DOI']]
            if temp_df['Query'].str.contains(flag).any():
                df_merged.loc[index, flag + '_flag'] = 1
    return df_merged


if __name__ == '__main__':
    main()
