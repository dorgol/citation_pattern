import streamlit as st
import pandas as pd
import pydeck as pdk

path = "C:/Users/dorgo/PycharmProjects/knowledge_dist/"
affiliations = pd.read_csv(path + 'affiliations.csv')

def read_yearly_citations(year):
    return pd.read_csv(path + 'citations_yearly/' + 'citations_yearly' + str(year) + '.csv')

def merge_loc(year, min_num, type_num = 'type_2'):
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

def create_hexmaps(year, min_num, type_num):
    # Specify a deck.gl ArcLayer
    arc_layer = pdk.Layer(
        "ColumnLayer",
        data=merge_loc(year, min_num),
        get_position=['Longitude_x', 'Latitude_x'],
        get_elevation=type_num,
        elevation_scale=100,
        radius=5000,
        get_fill_color=[255, 165, 0, 80],
        pickable=True,
        auto_highlight=True,

    )

    view = pdk.ViewState(latitude=39.155726,
                         longitude=-98.030561,
                         pitch=50,
                         zoom=3)
    # TOOLTIP_TEXT = {"html": "{" + type_num + "}"  + "citations  <br /> {DisplayName_x} citing - {DisplayName_y} cited <br /> "
    #                     "Citing institution in red; Cited institution in green"}
    st.pydeck_chart(pdk.Deck(arc_layer, initial_view_state=view, tooltip=True))

year = st.sidebar.slider('year', 1990, 1999, 1990, 1)
min_num = st.sidebar.slider('minimum citations', 5, 300, 10, 10)
type_num = st.sidebar.selectbox('type', ['type_1', 'type_2', 'type_3', 'type_4'])
create_hexmaps(type_num=type_num, year=year, min_num=min_num)
