import streamlit as st
import pandas as pd
import pydeck as pdk
import graph_aspects

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

GREEN_RGB = [0, 255, 0, 40]
RED_RGB = [240, 100, 0, 40]



def create_maps(year, min_num, type_num, scaling, method, quantile):
    # Specify a deck.gl ArcLayer
    G = graph_aspects.create_yearly_graph(year, year)
    data = graph_aspects.filtered_graph(year, G, method, quantile, min_num)
    arc_layer = pdk.Layer(
        "ArcLayer",
        data=data,
        get_width= type_num + "/" + str(scaling),
        get_source_position=["Longitude_x", "Latitude_x"],
        get_target_position=["Longitude_y", "Latitude_y"],
        get_tilt=15,
        get_source_color=RED_RGB,
        get_target_color=GREEN_RGB,
        pickable=True,
        auto_highlight=True,
    )

    view = pdk.ViewState(latitude=39.155726,
                         longitude=-98.030561,
                         pitch=50,
                         zoom=3)
    TOOLTIP_TEXT = {"html": "{" + type_num + "}"  + " citations. Centrality of citing institution: {centrality}  <br /> {DisplayName_x} citing - {DisplayName_y} cited <br /> "
                        "Citing institution in red; Cited institution in green"}
    # deck = pdk.Deck(arc_layer, initial_view_state=view, tooltip=TOOLTIP_TEXT)
    st.title("Pattern of Citations Between Institutions")
    st.pydeck_chart(pdk.Deck(arc_layer, initial_view_state=view, tooltip=TOOLTIP_TEXT))

year = st.sidebar.slider('year', 1990, 1999, 1990, 1)
min_num = st.sidebar.slider('minimum citations', 5, 300, 10, 10)
type_num = st.sidebar.selectbox('type', ['type_1', 'type_2', 'type_3', 'type_4'])
scaling = st.sidebar.slider('line width scaler', 10, 1000, 100, 10)
method = st.sidebar.selectbox('method', ['degree_centrality', 'in_degree_centrality', 'out_degree_centrality',
 'eigenvector_centrality', 'closeness_centrality', 'voterank', 'betweenness_centrality',
 'edge_betweenness_centrality', 'harmonic_centrality']
)
quantile = st.sidebar.slider('quantile', 0.05, 1.0, 0.5, 0.05)
create_maps(type_num=type_num, year=year, min_num=min_num, scaling=scaling, method=method, quantile=quantile)

# r.to_html("arc_layer.html")
