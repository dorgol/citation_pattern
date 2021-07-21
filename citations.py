import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from functools import reduce
import pickle



if __name__=='__main__':
    client = Client()
    print(client)


path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"


citation_pattern = dd.read_csv(path + 'citations/' + 'citation_pattern.csv', error_bad_lines=False)

sub_data = citation_pattern[['PaperId_r', 'AffiliationId_r', 'AuthorId_r', 'PaperReferenceId_r', 'Year_r']]

def cited_papers(year):
    # Goal: return all the cited papers in a given year.
    # Gets: year: int, the year in which the papers were cited
    # Returns: a dask dataframe with 4 columns - 'AffiliationId_r', 'AuthorId_r', 'PaperId_r', 'Year_r'

    data_yearly = sub_data[(sub_data['Year_r'] == year)]
    ref_yearly = data_yearly.loc[:, data_yearly.columns == 'PaperReferenceId_r']
    cited_yearly = data_yearly.merge(ref_yearly, left_on='PaperId_r', right_on='PaperReferenceId_r')
    cited_yearly = cited_yearly[cited_yearly.columns.difference(['PaperReferenceId_r_x', 'PaperReferenceId_r_y'])]
    cited_yearly = cited_yearly.drop_duplicates()
    return cited_yearly


def full_data(year_citing, year_cited):

    # Goal: the function return a dataframe with all the data about citing and cited papers in a pair of years.
    # Gets: year_citing: int, the publish year of the citing paper;
    #       year_cited: int, the publish year of the cited paper.
    # Returns: a dask dataframe with 8 columns: 'CitingId', 'CitingAffiliatoinId', 'CitingAuthorId', 'CitingYearId',
    #           'CitedId', 'CitedAffiliatoinId', 'CitedAuthorId', 'CitedYearId'.

    data_yearly = sub_data[(sub_data['Year_r'] == year_citing)]
    cited = cited_papers(year_cited)
    cited_yearly = data_yearly.merge(cited, left_on='PaperReferenceId_r', right_on='PaperId_r')
    cited_yearly = cited_yearly.loc[:, cited_yearly.columns != 'PaperReferenceId_r']
    cited_yearly.columns = ['CitingId', 'CitingAffiliatoinId', 'CitingAuthorId', 'CitingYearId',
                            'CitedAffiliatoinId', 'CitedAuthorId', 'CitedId', 'CitedYearId']
    return cited_yearly

def citation_amount(year_citing, year_cited, cal_type):


    # Goal: assign the number of citations between two affiliations in any year pair
    # Gets: year_citing: int, the publish year of the citing paper;
    #       year_cited: int, the publish year of the cited paper.
    #       cal_type: int, detailed below.
    # I perform this task in several ways -
    # Type 1 - count separately each pair of researchers. e.g. if a paper of 2 Harvard researchers cite a paper
    #  by 2 Cornell researchers it will count as 2*2*(Harvard->Cornell). It's the same for every composition -
    #  a multiplication of the sums
    # Type 2 - count every pair of affiliations per paper. In the same case as above I count Harvard and Cornell once.
    # If another paper who cites Cornell is a collaboration between Harvard and MIT researchers
    # I count it as one for Harvard (and not 0.5).
    # Type 3 - It's type 1 with normalization w.r.t the number of writers. If I have a paper written by
    #  2 Harvard researchers and 1 Cornell researcher I will count every citation as 0.33
    #  and I will sum across the dyads.
    # Type 4 - It's type 2 with normalization w.r.t the number of affiliations. If I have a paper written by
    #  2 Harvard researchers and 1 Cornell researcher I will count every citation
    #  as 0.5 and I will sum across the dyads.

    data = full_data(year_citing, year_cited)
    if cal_type == 1:
        amount = data.groupby(['CitingAffiliatoinId', 'CitedAffiliatoinId']).agg({'CitingAuthorId': 'count'})
    if cal_type == 2:
        data = data[['CitingId', 'CitingAffiliatoinId', 'CitedId', 'CitedAffiliatoinId']]
        data = data.drop_duplicates()
        amount = data.groupby(['CitingAffiliatoinId', 'CitedAffiliatoinId']).agg({'CitingId': 'count'})
    if cal_type == 3:
        data_writers = data[['CitingId', 'CitingAuthorId']].drop_duplicates()
        num_writers = data_writers.groupby('CitingId').agg({'CitingAuthorId': 'count'})
        data_by_writer = data.merge(num_writers, left_on='CitingId', right_on='CitingId')
        data_by_writer['CitingAuthorId_y'] = 1 / data_by_writer['CitingAuthorId_y']
        amount = data_by_writer.groupby(['CitingAffiliatoinId', 'CitedAffiliatoinId']).agg({'CitingAuthorId_y': 'sum'})
    if cal_type == 4:
        data_affiliations = data[['CitingId', 'CitingAffiliatoinId']].drop_duplicates()
        num_affiliations = data_affiliations.groupby('CitingId').agg({'CitingAffiliatoinId': 'count'})
        data_by_affiliation = data.merge(num_affiliations, left_on='CitingId', right_on='CitingId')
        data_by_affiliation['CitingAffiliatoinId_y'] = 1 / data_by_affiliation['CitingAffiliatoinId_y']
        amount = data_by_affiliation.groupby(['CitingAffiliatoinId_x', 'CitedAffiliatoinId']).agg({'CitingAffiliatoinId_y': 'sum'})
    amount['year_citing'] = year_citing
    amount['year_cited'] = year_cited
    amount = amount.rename(columns={amount.columns[0]: "number"})
    return amount


def citation_by_years(year_cited, year_citing, cal_type):

    # Goal: get a dask dataframe of all cited papers from a given range
    # Gets: year_citing: int, the publish year of the citing paper;
    #       year_cited: int, the publish year of the cited paper.
    #       cal_type: int, detailed below.
    # Returns: dask dataframe of the citations in the range of supplied years.
    lis = []
    for year in range(year_cited, year_citing+1):
        citations = citation_amount(year_citing, year, cal_type=cal_type)
        lis.append(citations)
    dfs = dd.concat(lis)
    return dfs

def citation_all_types(year_cited, year_citing):
    empty_lis = []
    for i in [1, 2, 3, 4]:
        a = citation_by_years(year_cited, year_citing, i).compute()
        a = a.reset_index()
        a.rename(columns={a.columns[0]: 'CitingAffiliatoinId'}, inplace=True)
        empty_lis.append(a)
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['CitingAffiliatoinId', 'CitedAffiliatoinId',
                                                                     'year_citing', 'year_cited'],
                                                    how='outer'), empty_lis)
    df_merged.columns = ['CitingAffiliatoinId', 'CitedAffiliatoinId', 'type_1', 'year_citing',
                         'year_cited', 'type_2', 'type_3', 'type_4']
    return df_merged

def citation_types_years(year_citing):
    years = range(1990, year_citing + 1)
    lis = []
    for i in years:
        unique_df = citation_all_types(i, year_citing)
        lis.append(unique_df)
    df = pd.concat(lis).drop_duplicates()
    return df

def save_citation_types(end_year):
    year_range = range(1990, end_year + 1)
    citations_yearly = citation_types_years(year_range[len(year_range) - 1])
    name_db = path + 'citations_yearly/' + 'citations_yearly' + str(end_year) + '.csv'
    with open(name_db, "wb") as fp:  # Pickling
        pickle.dump(citations_yearly, fp)

def saving_all_years():
    #Goal: save citations_yearly for every year
    years = list(range(1990,2001))
    for i in years:
        save_citation_types(i)


def citation_all_years(cal_type):

    # Goal: get a dask dataframe of all cited papers from all of the relevant year pairs.
    # Gets: cal_type: int, detailed above.
    # Returns: dask dataframe of the citations in the range of the entire year range.
    years = list(range(1990,2001))
    full_citation = []
    for i in years:
        by_year = citation_all_types(year_cited=i, year_citing=2000)
        full_citation.append(by_year)
    df = dd.concat(full_citation)
    return df


def unique_citing_aff(year_citing, year_cited):
    df = full_data(year_citing, year_cited).reset_index()
    df = df[['CitingAffiliatoinId', 'CitedAffiliatoinId', 'CitingYearId', 'CitedYearId']].drop_duplicates()
    return df

def unique_citing_year(year_citing):
    years = range(1990,year_citing + 1)
    lis = []
    for i in years:
        unique_df = unique_citing_aff(year_citing, i).compute()
        lis.append(unique_df)
    df = pd.concat(lis).drop_duplicates()
    return df

def reading_yearly_data(citing_year, file_type, cited_year = None):
    if file_type == 'paths':
        folder = 'shortest_paths'
        relative_path = path + folder + '/' + 'citing_path' + str(citing_year) + '_' + str(cited_year) + '.txt'
        with open(relative_path, "rb") as fp:  # Unpickling
            df = pickle.load(fp)
    elif file_type == 'citations':
        folder = 'citations_yearly'
        relative_path = path + folder + '/' + 'citations_yearly' + str(citing_year) + '.csv'
        df = pd.read_csv(relative_path)
        df.drop(['Unnamed: 0'], axis = 1, inplace=True)
    else:
        'file_type has to be "paths" or "citations_yearly"'
    return df

def reading_data(year, starting_year = 1990, paths = False):
    paths_length = []
    actual_paths = []
    for i in range(starting_year, year + 1):
        df_path = reading_yearly_data(year, 'paths', i)
        df_path1 = [df_path[1][i][1] for i in range(len(df_path[1]))]
        paths_length.append(df_path1)
        if paths == True:
            df_path2 = [df_path[1][i][0] for i in range(len(df_path[1]))]
            actual_paths.append(df_path2)
    return (paths_length, actual_paths)

# a = reading_data(1999, 1990, True)

if __name__=='__main__':
    saving_all_years()







