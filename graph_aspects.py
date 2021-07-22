import networkx as nx
import pandas as pd
from arc_layer import merge_loc
from networkx.algorithms import approximation


path = "https://raw.githubusercontent.com/dorgol/citation_pattern/main/affiliations.csv"
affiliations = pd.read_csv(path)

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

g90 = create_yearly_graph(1990, 1990)

#centrality
def graph_centrality(G, method, **kwargs):
    centrality = method(G, **kwargs)
    if type(centrality) != list:
        centrality = sorted(((c, v) for v, c in centrality.items()), reverse=True)
    else:
        centrality = list(enumerate(centrality))
    centrality = pd.DataFrame(centrality, columns=['centrality', 'AffiliationId'])
    return centrality

def filtered_graph(year, G, method, quantile, type_num = 'type_2'):
    data = merge_loc(year, 0, type_num)
    centrality = graph_centrality(G, method)
    data = pd.merge(data, centrality, left_on='CitingAffiliatoinId',right_on ='AffiliationId')
    data = data[data.centrality < data.centrality.quantile(quantile)]
    return data

filtered_graph(1990, g90, nx.in_degree_centrality, 0.95)

a = graph_centrality(g90,nx.in_degree_centrality)


# no_weight = ['degree_centrality', 'in_degree_centrality', 'out_degree_centrality',
#  'eigenvector_centrality', 'closeness_centrality', 'voterank']
# yes_weight = ['betweenness_centrality', 'edge_betweenness_centrality', 'harmonic_centrality']












#TODO: community
#TODO: cliques
#TODO: clustering



approximation.max_clique(g90)
approximation.average_clustering(g90)
nx.degree_assortativity_coefficient(g90, x='in', y='out', weight='type_2')
nx.degree_pearson_correlation_coefficient(g90, weight='type_2')


