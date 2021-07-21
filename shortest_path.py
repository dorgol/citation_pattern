import time
import dask.dataframe as dd
import glob
from dask.distributed import Client, LocalCluster
import pandas as pd
import networkx as nx
import timeit
import citations
import multiprocessing as mp
import pickle
import igraph as ig

if __name__=='__main__':
    client = Client()
    print(client)

# client = Client()
# print(client)


path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"

closest_airports = pd.read_csv(path + 'closest_airports/' + 'closest_airports.csv')
closest_affiliations = pd.read_csv(path + 'closest_affiliations/' + 'closest_affiliations.csv')

path_cite = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"

citation_pattern = dd.read_csv(path_cite + 'citations/' + 'citation_pattern.csv', error_bad_lines=False)

cols = ['PASSENGERS', 'FREIGHT', 'MAIL', 'DISTANCE', 'UNIQUE_CARRIER',
        'AIRLINE_ID', 'UNIQUE_CARRIER_NAME', 'UNIQUE_CARRIER_ENTITY', 'REGION',
        'CARRIER', 'CARRIER_NAME', 'CARRIER_GROUP', 'CARRIER_GROUP_NEW',
        'ORIGIN_AIRPORT_ID', 'ORIGIN_AIRPORT_SEQ_ID', 'ORIGIN_CITY_MARKET_ID',
        'ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR', 'ORIGIN_STATE_FIPS',
        'ORIGIN_STATE_NM', 'ORIGIN_WAC', 'DEST_AIRPORT_ID',
        'DEST_AIRPORT_SEQ_ID', 'DEST_CITY_MARKET_ID', 'DEST', 'DEST_CITY_NAME',
        'DEST_STATE_ABR', 'DEST_STATE_FIPS', 'DEST_STATE_NM', 'DEST_WAC',
        'YEAR', 'QUARTER', 'MONTH', 'DISTANCE_GROUP', 'CLASS', 'Unnamed: 36']

dtypes = {'PASSENGERS': float, 'FREIGHT': float, 'MAIL': float, 'DISTANCE': float,
          'YEAR': float, 'QUARTER': float, 'MONTH': float, 'DISTANCE_GROUP': float,
          'AIRLINE_ID': str, 'CARRIER_GROUP_NEW': str}



def reading_flights_data(dtype=dtypes):
    # Goal: reading flights data
    # Gets: dtypes: list. List of data types of data columns.
    # Returns: loaded data.

    all_files = glob.glob(path + "flights/" + "*.csv")
    return dd.read_csv(all_files, names=cols, dtype=dtype, header=0)


def travel_time(df, col, miles_to_km, avg_speed, min_time):
    # Goal: Add a travel time column to the data.
    # Gets: df: Dataframe (dask or pandas): The dataframe to add the column to.
    #      col: string. the column from which to calculate the travel time.
    #      miles_to_km: float. If the data is in miles it should get 1.6. If it's in kms it should get 1.
    #      avg_speed: float. What is the average speed of the relevant vehicle. e.g, 60 km/h for a car.
    #      min_time: int. What is the minimum travel time of the vehicle.
    #                     Its purpose is to give a floor to flight time.
    # Returns: df. Dataframe with one column of travel time.
    dist = df[col]
    dist = dist * miles_to_km
    time = dist / avg_speed * 60
    time = time.clip(lower=min_time)
    return time


def loc_to_graph(df, dest_col, org_col, dist_col, df_type, year_col='YEAR', month_col='MONTH'):
    # Goal: Taking a dataframe of origin and destination and adapt it to fit to graph format.
    # Gets: df: dataframe. The dataframe of origins and destinations.
    #       dest_col: string. The name of the destination column in the dataframe.
    #       dist_col: string. The name of the distance column in the dataframe.
    #       org_col: string. The name of the origin column in the dataframe.
    #       year_col: string. The name of the year column in the dataframe. Default: 'YEAR'.
    #       month_col: string. The name of the month column in the dataframe. Default: 'MONTH'.
    #       df_type: string. Can be either pandas or dask.
    # Returns: df. A dataframe in a format the fits graph data.
    if df_type == 'pandas':
        df_new = df[[dest_col, org_col, dist_col]]
        df_new = dd.from_pandas(df_new, npartitions=2 * mp.cpu_count())
        new_columns = ['origin', 'destination', 'distances']
        df_new = df_new.rename(columns=dict(zip(df_new.columns, new_columns)))
    if df_type == 'dask':
        df_new = df[[dest_col, org_col, dist_col, year_col, month_col]]
        new_columns = ['origin', 'destination', 'distances', 'year', 'month']
        df_new = df_new.rename(columns=dict(zip(df_new.columns, new_columns)))
    return df_new


def entire_ways_df(years_span):
    # Goal: append all the relevant data to a travel graph. It should contain the ways from affiliations and airports
    #       in every direction
    # Gets: years_span: list of ints. The years in the data.
    # Returns: dataframe. A dataframe with all the ways between affiliations and airports.

    flights_data = reading_flights_data()

    affiliations_to_airports = loc_to_graph(df=closest_airports, dest_col='airports', org_col='affiliationId',
                                            dist_col='distances', df_type = 'pandas')
    airports_to_affiliations = loc_to_graph(df=closest_airports, dest_col='affiliationId', org_col='airports',
                                            dist_col='distances', df_type = 'pandas')
    airport_to_airport = loc_to_graph(df=flights_data, dest_col='ORIGIN_CITY_NAME', org_col='DEST_CITY_NAME',
                                      dist_col='DISTANCE', year_col='YEAR', month_col='MONTH', df_type='dask')
    affiliations_to_affiliations = loc_to_graph(df=closest_affiliations, dest_col='AffiliationId', org_col='affiliationId',
                                            dist_col='distances', df_type='pandas')

    unique_aff = affiliations_to_airports[['origin']].drop_duplicates().compute()
    unique_aff = [i[0] for i in unique_aff.values]

    years_airports = airport_to_airport[airport_to_airport['year'] == years_span]


    affiliations_to_airports = affiliations_to_airports[affiliations_to_airports['origin'].isin(unique_aff)]
    airports_to_affiliations = airports_to_affiliations[airports_to_affiliations['destination'].isin(unique_aff)]
    affiliations_to_affiliations = affiliations_to_affiliations[
        affiliations_to_affiliations['destination'].isin(unique_aff)]
    affiliations_to_affiliations = affiliations_to_affiliations[
        affiliations_to_affiliations['origin'].isin(unique_aff)]

    airports_to_affiliations['travel_time'] = travel_time(df=airports_to_affiliations, col='distances',
                                                          miles_to_km=1, avg_speed=60,
                                                          min_time=0)
    affiliations_to_airports['travel_time'] = travel_time(df=affiliations_to_airports, col='distances',
                                                          miles_to_km=1, avg_speed=60,
                                                          min_time=0)

    years_airports['travel_time'] = travel_time(df=years_airports, col='distances',
                                                 miles_to_km=1.6, avg_speed=900,
                                                 min_time=30)
    affiliations_to_affiliations['travel_time'] = travel_time(df=affiliations_to_affiliations, col='distances',
                                                          miles_to_km=1, avg_speed=60,
                                                          min_time=0)
    all_ways = dd.concat([years_airports, affiliations_to_airports, airports_to_affiliations,
                          affiliations_to_affiliations], axis=0)

    return all_ways

def yearly_graph(year):
    df = entire_ways_df(year)
    df_yearly = df.groupby(['destination', 'origin'], group_keys=False).agg(
        {'year': ['mean'], 'travel_time': 'mean'}).reset_index()
    df_yearly = df_yearly.compute(scheduler='processes')
    df_yearly.columns = df_yearly.columns.droplevel(1)
    df_yearly = df_yearly.drop(columns = ['year'])
    df_yearly[['destination', 'origin']] = df_yearly[['destination', 'origin']].astype(str)
    G = ig.Graph.TupleList(df_yearly.itertuples(index=False), directed=True, weights=False, edge_attrs="travel_time")
    return G


def find_dijkstra(df, graph):
    try:
        source = str(int(df['CitingAffiliatoinId']))
        target = str(int(df['CitedAffiliatoinId']))
        source_idx = graph.vs.find(name=source).index
        target_idx = graph.vs.find(name=target).index
        weight = graph.es["travel_time"]
        # length = graph.shortest_paths(source=source_idx, target=target_idx, weights=weight)[0][0]
        path = graph.get_shortest_paths(source_idx,target_idx, weights = weight, output='epath')
        path_length = 0
        for i in path[0]:
            vpath = graph.es.find(i)['travel_time']
            path_length += vpath

    except:
        path_length = float('inf')
        path = []
    return path, path_length




def finding_routes(year, citations_yearly):
    graph = yearly_graph(year)
    distance_calc = dd.from_pandas(citations_yearly, npartitions=2 * mp.cpu_count()) \
            .map_partitions(lambda df: df.apply(find_dijkstra, graph = graph, axis = 1))\
            .compute(scheduler = 'processes')

    return path, distance_calc


def finding_routes_yearly(end_year):

    # year_range = range(1990, end_year+1)
    year_range = range(1990, end_year + 1)
    citations_yearly = pd.read_csv(path + 'citations_yearly/' + 'citations_yearly' + str(end_year) + '.csv')
    for idx, year in list(enumerate(year_range, 1)):
        start_time = timeit.default_timer()
        print("The start time of year", year, "is:", start_time)
        citations_and_time = finding_routes(year, citations_yearly)
        name_list = path + 'shortest_paths/' +'citing_path' + str(end_year) + '_' + str(year) + '.txt'
        with open(name_list, "wb") as fp:  # Pickling
            pickle.dump(citations_and_time, fp)
        print("The time difference is :", timeit.default_timer() - start_time, "seconds")



# finding_routes_yearly(1999)


def main():
    start_time = timeit.default_timer()
    print("The start time is :", start_time)
    years = range(1990,2000)
    for year in years:
        finding_routes_yearly(year)
    print("The time difference is :", timeit.default_timer() - start_time, "seconds")


if __name__=='__main__':
    main()