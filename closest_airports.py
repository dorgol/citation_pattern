import math
import pandas as pd
import ast
from scipy import spatial

path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"
airports_location = pd.read_csv(path+'airports_location/'+'airports_location.csv')
airports_location.airports_locations = airports_location.airports_locations.apply(ast.literal_eval)
affiliations = pd.read_csv(path+'affiliations/'+'affiliations.csv')
affiliations['coordinates'] = affiliations[['Latitude', 'Longitude']].values.tolist()

def cartesian(coordinates):

    latitude = coordinates[0]
    longitude = coordinates[1]

    # Convert to radians
    latitude = latitude * (math.pi / 180)
    longitude = longitude * (math.pi / 180)

    R = 6371 # 6378137.0 + elevation  # relative to centre of the earth
    X = R * math.cos(latitude) * math.cos(longitude)
    Y = R * math.cos(latitude) * math.sin(longitude)
    Z = R * math.sin(latitude)
    return X, Y, Z


def closest_institutions(affiliationId, inst, coordinates_col, k = 5):
    subset = affiliations[affiliations.AffiliationId == affiliationId]['coordinates']
    subset = subset.apply(cartesian).tolist()
    cartesian_coord = coordinates_col.apply(cartesian)
    tree = spatial.KDTree(list(cartesian_coord))
    closest = tree.query([subset], p=2, k=k)
    index = closest[1][0]
    distances = pd.Series(closest[0][0].flatten())
    k_close = inst.iloc[index.flatten()]
    k_close.loc[:,'distances'] = distances.values
    affiliationName = affiliations[affiliations.AffiliationId == affiliationId]['DisplayName'].values
    k_close['affiliationName'] = affiliationName[0]
    k_close['affiliationId'] = affiliationId
    return k_close


def main_op():

    k_closest_affiliations = pd.concat([closest_institutions(i, affiliations, affiliations.coordinates, 2)
                                        for i in affiliations.AffiliationId])
    k_closest_affiliations = k_closest_affiliations[k_closest_affiliations.affiliationId != k_closest_affiliations.AffiliationId]
    k_closest_airports = pd.concat(
        [closest_institutions(i, airports_location, airports_location.airports_locations, 2)
         for i in affiliations.AffiliationId])
    k_closest_affiliations.to_csv(path + 'closest_affiliations/' + 'closest_affiliations.csv', index=False)
    k_closest_airports.to_csv(path + 'closest_airports/' + 'closest_airports.csv', index=False)

main_op()




