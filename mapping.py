"""
ArcLayer
========

Map of commutes to work within a segment of downtown San Francisco using a deck.gl ArcLayer.

Green indicates a start point, and red indicates the destination.

The data is collected by the US Census Bureau and viewable in the 2017 LODES data set: https://lehd.ces.census.gov/data/
"""

import pydeck as pdk
import pandas as pd

MAPBOX_API_KEY  = 'pk.eyJ1IjoiZG9yZ29sZGVuYmVyZyIsImEiOiJja3I5Y2V0b280NHk1Mm9xaDZtNGZ1bjNqIn0.Xk_uoM-k27rXtmdCf-MI5g'

# ArcLayer - work commute patterns (create dataframe with fake data)
# to change look of commute lines use arguments such as pitch, get_width, get_tilt, etc.

# data
# data = 'https://raw.githubusercontent.com/groundhogday321/dataframe-datasets/master/us_county_centroids.csv'
# county_population = pd.read_csv(data)

county_population['scaled_population'] = county_population['Population_2010']/1_000
print(county_population.columns)

# view (location, zoom level, etc.)
view = pdk.ViewState(latitude=39.155726, longitude=-98.030561, pitch=50, zoom=3)

# layer
column_layer = pdk.Layer('ColumnLayer',
                         data=county_population,
                         get_position=['Longitude', 'Latitude'],
                         get_elevation='scaled_population',
                         elevation_scale=100,
                         radius=5000,
                         get_fill_color=[255, 165, 0, 80],
                         pickable=True,
                         auto_highlight=True)

# render map
# with no map_style, map goes to default
column_layer_map = pdk.Deck(layers=column_layer,
                            initial_view_state=view,
                            api_keys={'mapbox': MAPBOX_API_KEY})

# display and save map (to_html(), show())
column_layer_map.to_html('temp.html')