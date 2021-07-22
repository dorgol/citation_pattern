import streamlit as st
import networkx as nx

K5 = nx.complete_graph(5)
dot = nx.nx_pydot.to_pydot(K5)
st.graphviz_chart(dot.to_string())