from dask.distributed import Client
import pandas as pd
import dask.dataframe as dd

class data_to_read():
    path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"

    closest_airports = pd.read_csv(path + 'closest_airports/' + 'closest_airports.csv')

    path_cite = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"

    citation_pattern = dd.read_csv(path_cite + 'citations/' + 'citation_pattern.csv', error_bad_lines=False)

    sub_data = citation_pattern[['PaperId_r', 'AffiliationId_r', 'AuthorId_r', 'PaperReferenceId_r', 'Year_r']]

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




