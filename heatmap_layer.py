import pandas as pd
import pydeck as pdk
import arc_layer
import streamlit as st
from palettable.cartocolors.sequential import BrwnYl_3

MAPBOX_API_KEY  = 'pk.eyJ1IjoiZG9yZ29sZGVuYmVyZyIsImEiOiJja3I5Y2V0b280NHk1Mm9xaDZtNGZ1bjNqIn0.Xk_uoM-k27rXtmdCf-MI5g'
path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"
affiliations = pd.read_csv(path + 'affiliations/' + 'affiliations.csv')

heatmap_layer = pdk.Layer('HeatmapLayer',
                          data=affiliations,
                          opacity=0.9,
                          get_position=['Longitude', 'Latitude'],
                          color_range=BrwnYl_3.colors,
                          threshold=0.2,
                          get_weight='CitationCount',
                          pickable=True)

view = pdk.ViewState(latitude=39.155726,
                     longitude=-98.030561,
                     pitch=50,
                     zoom=3)

# render map
heatmap_layer_map = pdk.Deck(layers=heatmap_layer,
                            initial_view_state=view,
                            api_keys={'mapbox': MAPBOX_API_KEY})