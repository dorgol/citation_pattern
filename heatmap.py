import streamlit as st
import pandas as pd
import pydeck as pdk

path = "https://raw.githubusercontent.com/dorgol/citation_pattern/main/affiliations.csv"
affiliations = pd.read_csv(path)

def read_yearly_citations(year):
    return pd.read_csv('https://raw.githubusercontent.com/dorgol/citation_pattern/main/citations_yearly/citations_yearly' +
                       str(year) + '.csv')

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

def create_heatmaps(year, min_num, type_num):
    # Specify a deck.gl ArcLayer
    arc_layer = pdk.Layer(
        "HeatmapLayer",
        data=merge_loc(year, min_num),
        opacity=0.9,
        get_position=["Longitude_x", "Latitude_x"],
        threshold=0.1,
        aggregation=pdk.types.String("MEAN"),
        get_weight=type_num,
        pickable=True,
    )

    view = pdk.ViewState(latitude=39.155726,
                         longitude=-98.030561,
                         pitch=50,
                         zoom=3)
    # deck = pdk.Deck(arc_layer, initial_view_state=view, tooltip=TOOLTIP_TEXT)
    st.title("Heatmap of Research Papers per Year")
    st.pydeck_chart(pdk.Deck(arc_layer, initial_view_state=view,
                             tooltip={"text": "concentration of research"}))


year = st.sidebar.slider('year', 1990, 1999, 1990, 1)
min_num = st.sidebar.slider('minimum citations', 5, 300, 10, 10)
type_num = st.sidebar.selectbox('type', ['type_1', 'type_2', 'type_3', 'type_4'])
# scaling = st.sidebar.slider('line width scaler', 10, 1000, 100, 10)
create_heatmaps(type_num=type_num, year=year, min_num=min_num)
