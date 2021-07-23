import networkx as nx
import pandas as pd

path = "https://raw.githubusercontent.com/dorgol/citation_pattern/main/affiliations.csv"
affiliations = pd.read_csv(path)

def read_yearly_citations(year):
    #Goal: read the citations data in a given year
    #Gets: year; int. the desired year of the data.
    #Returns: dataframe. Read 'Goal'.
    return pd.read_csv('https://raw.githubusercontent.com/dorgol/citation_pattern/main/citations_yearly/citations_yearly' +
                       str(year) + '.csv')

def merge_loc(year, min_num, type_num = 'type_2'):
    #Goal: merge the citations data with a location of every institution.
    #Gets: year; int. The desired year of the data.
    #      min_num; int. The minimal number of citations that will be included in the data.
    #      type_num: str; either 'type_1', 'type_2', 'type_3' or 'type_4'. The type of citation.
    #      For more information read the description in 'citations.citation_amount'.
    #Returns: dataframe; read the 'Goal'.
    df = read_yearly_citations(year)
    a = pd.merge(df, affiliations, 'inner',
             left_on='CitingAffiliatoinId',
             right_on='AffiliationId',
             left_index=False,
             right_index=False
             )
    a = a.dropna()
    b = pd.merge(a, affiliations, 'inner',
             left_on='CitedAffiliatoinId',
             right_on='AffiliationId',
             left_index=False,
             right_index=False
             )
    b = b.dropna()
    b = b[b[type_num] >= min_num]
    return b

def loc_all_years(start_year = 1990, end_year = 1999, min_num = 0, type_num = 'type_2'):
    years_range = range(start_year,end_year+1)
    lis = []
    for year in years_range:
        a = merge_loc(year, min_num, type_num)
        lis.append(a)
    return pd.concat(lis)

def create_yearly_graph(start_year, end_year, type_num = 'type_2'):
    df = loc_all_years(start_year, end_year, min_num = 0, type_num = 'type_2')
    df = df.dropna()
    aff = affiliations.drop(['NormalizedName', 'GridId', 'OfficialPage',
                             'WikiPage', 'Iso3166Code', 'CreatedDate'],
                            axis = 1)
    aff = aff.set_index('AffiliationId').to_dict('index')
    G = nx.from_pandas_edgelist(df=df, source='CitingAffiliatoinId', target='CitedAffiliatoinId',
                                edge_attr=type_num, create_using=nx.DiGraph)
    nx.set_node_attributes(G, aff)
    return G

#centrality
def graph_centrality(G, method, **kwargs):
    func = eval('nx.' + method)
    centrality = func(G, **kwargs)
    if type(centrality) != list:
        centrality = sorted(((c, v) for v, c in centrality.items()), reverse=True)
    else:
        centrality = list(enumerate(centrality))
    centrality = pd.DataFrame(centrality, columns=['centrality', 'AffiliationId'])
    return centrality

def filtered_graph(year, G, method, quantile, min_num, type_num = 'type_2'):
    data = merge_loc(year, min_num, type_num)
    centrality = graph_centrality(G, method)
    data = pd.merge(data, centrality, left_on='CitingAffiliatoinId',right_on ='AffiliationId')
    data = data[data.centrality > data.centrality.quantile(quantile)]
    return data


# no_weight = ['degree_centrality', 'in_degree_centrality', 'out_degree_centrality',
#  'eigenvector_centrality', 'closeness_centrality', 'voterank']
# yes_weight = ['betweenness_centrality', 'edge_betweenness_centrality', 'harmonic_centrality']

#TODO: community
#TODO: cliques
#TODO: clustering
