# from dask.distributed import Client
# import dask.dataframe as dd
import glob
from dask.distributed import Client
import pandas as pd


client = Client()
print(client)

path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"

all_files = glob.glob(path + "flights/" + "*.csv")
us_airports = pd.read_csv(path + 'airports_location/us-airports.csv')


cols = ['PASSENGERS', 'FREIGHT', 'MAIL', 'DISTANCE', 'UNIQUE_CARRIER',
       'AIRLINE_ID', 'UNIQUE_CARRIER_NAME', 'UNIQUE_CARRIER_ENTITY', 'REGION',
       'CARRIER', 'CARRIER_NAME', 'CARRIER_GROUP', 'CARRIER_GROUP_NEW',
       'ORIGIN_AIRPORT_ID', 'ORIGIN_AIRPORT_SEQ_ID', 'ORIGIN_CITY_MARKET_ID',
       'ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR', 'ORIGIN_STATE_FIPS',
       'ORIGIN_STATE_NM', 'ORIGIN_WAC', 'DEST_AIRPORT_ID',
       'DEST_AIRPORT_SEQ_ID', 'DEST_CITY_MARKET_ID', 'DEST', 'DEST_CITY_NAME',
       'DEST_STATE_ABR', 'DEST_STATE_FIPS', 'DEST_STATE_NM', 'DEST_WAC',
       'YEAR', 'QUARTER', 'MONTH', 'DISTANCE_GROUP', 'CLASS', 'Unnamed: 36']

dtypes = {'PASSENGERS': str, 'FREIGHT': str, 'MAIL': str, 'DISTANCE': str,
          'YEAR': str, 'QUARTER': str, 'MONTH': str, 'DISTANCE_GROUP': str,
          'AIRLINE_ID': str, 'CARRIER_GROUP_NEW': str}

def read_pandas():
    uniques = []
    for file in all_files:
        df = pd.read_csv(file)
        unique_col = df[['DEST', 'ORIGIN']]
        uniques.append(unique_col)
    dfs = pd.concat(uniques)
    a = dfs['DEST'].append(dfs['ORIGIN']).reset_index(drop=True)
    return a.unique()




# def reading_data(dtype=dtypes):
#        return dd.read_csv(all_files, names=cols, dtype=dtype)

# def unique_airports(data, col):
#        relevant_col = data[col]
#        unique_col = pd.DataFrame(relevant_col.drop_duplicates().compute())
#        unique_col = unique_col[1:]
#        unique_col = unique_col.rename(columns={col: 'airports'})
#        return unique_col

# def locate(address):
#     from geopy import Nominatim
#     geolocator = Nominatim(user_agent="Dor_Goldenberg")
#     location = geolocator.geocode(address, timeout=3)
#     try:
#         coord = (location.latitude, location.longitude)
#     except:
#         address_provided = address.split('/', 1)[0]
#         location = geolocator.geocode(address_provided, timeout=3)
#         coord = (location.latitude, location.longitude)
#     return address, coord

def main_op():
       # df = reading_data()
       # unique_dest = unique_airports(df, 'DEST_CITY_NAME')
       # unique_origin = unique_airports(df, 'ORIGIN_CITY_NAME')
       # airports = unique_origin.append(unique_dest)
       # airports = pd.DataFrame(airports.airports.unique())
       # airports.columns = ['blah']
       # airports[airports.blah.str.contains('\w')]
       # l = [locate(location) for location in airports]
       # airports_locations = pd.DataFrame(l, columns=['airports', 'airports_locations'])
       dfs = read_pandas()
       airports_locations = us_airports[us_airports.iata_code.isin(dfs)]
       airports_locations.to_csv(path+'airports_location/'+'airports_location.csv', index=False)

if __name__ == '__main__':
       main_op()


