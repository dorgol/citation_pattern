import pandas as pd
import numpy as np
from plotly import graph_objects as go
import plotly.express as px
import plotly
import distances_description
path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/citations_yearly/citations_yearly"


def read_data_year(year):
    return pd.read_csv(path + str(year) + '.csv', )

def read_data():
    years_range = range(1990,2000)
    dfs = []
    for year in years_range:
        df = read_data_year(year)
        dfs.append(df)
    data = pd.concat(dfs, axis=0, ignore_index=True)
    data.drop(data.columns[0], axis = 1, inplace=True)
    return data

def pdf_cdf(df, year, type_num, bins = 10):

    df = df[df['year_citing'] == year]
    series_len = df[type_num]
    series_len = series_len.values
    count, bins_count = np.histogram(series_len, bins=bins)

    # finding the PDF of the histogram using count values
    pdf = count / sum(count)

    # using numpy np.cumsum to calculate the CDF
    # We can also find using the PDF values by looping and adding
    cdf = np.cumsum(pdf)
    return cdf, pdf, bins_count

def pdf_cdf_all_years(df, type_num, bins = 100):
    years_range = range(1990,2000)
    cdfs = []
    pdfs = []
    bins_counts = []
    for year in years_range:
        tmp = pdf_cdf(df=df, year=year, type_num=type_num, bins=bins)
        cdfs.append(tmp[0])
        pdfs.append(tmp[1])
        bins_counts.append(tmp[2])
    return pdfs, cdfs, bins_counts

def plot_pcdf(data, type_dist, type_name, save = False, name = None):
    #Goal: plotting the distribution of time travel - pdf and cdf
    #Gets: data: dataframe. A dataframe of time travels. The product of 'pdf_cdf_all_years'.
    #      type_dist: int, 'cdf' for cdf, or 'pdf' for pdf.
    #      save: boolean, whether to save the plot.
    #      name: str, if save is true it's the name of the file.
    #Returns: plots the pdf/cdf. Saves the plot (optional)
    years_range = range(1990,2000)
    if type_dist == 'pdf':
        type_num = 0
    else:
        type_num = 1
    fig = go.Figure()
    for i in range(len(data[0])):
        fig.add_trace(go.Scatter(x=data[2][i], y=data[type_num][i],
                                 mode='lines',
                                 name=years_range[i]))
    fig.update_layout(
        title={
            'text': type_dist + ' of citations of ' + type_name,
            'y': 0.9,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="log number of citations",
        yaxis_title="density",
        legend_title="years",
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="RebeccaPurple"
        )
    )
    fig.update_xaxes(type="log")
    fig.show()
    if save == True:
        fig.write_html('C:/Users/dorgo/Documents/university/thesis/new_thesis/plots/' + name + '.html')

def plot_sum(df, type_num, facet_by, save = False, name = None):

    import plotly.express as px
    fig = px.histogram(df, x=type_num, log_y=True, nbins=1000,
                       facet_col=facet_by, facet_col_wrap=2)
    fig.update_xaxes(range=[0, 2000])
    if save == False:
        fig.show()
    elif save == True:
        fig.write_html('C:/Users/dorgo/Documents/university/thesis/new_thesis/plots/' + name + '.html')

def avg_cited(col_selected, **kwargs):
    years_range = range(1990,1999)
    df = read_data()
    for year in years_range:
        df_year = df[df[col_selected] == year]
        plot_sum(df_year, **kwargs)

def create_density_mapbox(df, col_selected, type_num,  agg_type, save = False, name = None):
    optional_cols = ['year_cited', 'year_citing']
    unselected = optional_cols.pop(optional_cols.index(col_selected))
    selected = optional_cols[0]
    fig = px.density_heatmap(df, x=unselected, y=selected,
                             z=type_num, histfunc=agg_type,
                             color_continuous_scale=plotly.colors.sequential.Oranges)
    fig.update_layout(
        title=dict(
            text='density mapbox of citations of ' + type_num + '\nfor every pair of years',
            y=1,
            x=0.5,
            font=dict(
                size=20,
                color='#000000'
            ),
            xanchor='center',
            yanchor='top'),
        yaxis_title=agg_type,
        font=dict(
            family="Courier New, monospace",
            size=11,
            color="RebeccaPurple"
        )
    )
    if save == True:
        fig.write_html('C:/Users/dorgo/Documents/university/thesis/new_thesis/plots/' + name + '.html')
    else:
        fig.show()

def create_histogram(df, col_selected, type_num, agg_type, save = False, name = None):
    optional_cols = ['year_cited', 'year_citing']
    unselected = optional_cols.pop(optional_cols.index(col_selected))
    selected = optional_cols[0]

    fig = px.histogram(df, x=selected, y = type_num, facet_col=unselected,
                       color= unselected,
                       log_y=True, nbins=10, histfunc=agg_type)
    fig.update_layout(
        title=dict(
            text=agg_type + ' of citations of' + type_num + 'over the years',
            y=1,
            x=0.5,
            font=dict(
                size=20,
                color='#000000'
            ),
            xanchor =  'center',
            yanchor =  'top'),
        yaxis_title=agg_type,
        font=dict(
            family="Courier New, monospace",
            size=11,
            color="RebeccaPurple"
        )
    )
    if save == True:
        fig.write_html('C:/Users/dorgo/Documents/university/thesis/new_thesis/plots/' + name + '.html')
    else:
        fig.show()

def main():
    df = read_data()
    type_names = ['type_1', 'type_2', 'type_3', 'type_4']
    type_calc = ['sum', 'count']
    optional_cols = ['year_citing', 'year_cited']
    for i in type_names:
        a = pdf_cdf_all_years(df, i, bins=100)
        plot_pcdf(a, 'pdf', i, True, 'pdf_citations' + i)
        plot_pcdf(a, 'cdf', i, True, 'cdf_citations' + i)
    for i in type_names:
        plot_sum(df, i, 'year_citing')
    # avg_cited(col_selected='year_cited', type_num='type_1', facet_by='year_citing', save=False, name=None)
    for i in type_names:
        for j in type_calc:
            for k in optional_cols:
                create_histogram(df=df, col_selected = k, type_num = i, agg_type = j,
                                 save=True, name= 'hist' + i + '_' + j + '_' + k)

                create_density_mapbox(df=df, col_selected=k, type_num=i, agg_type=j,
                                 save=True, name='mapbox' + i + '_' + j + '_' + k)

if __name__=='__main__':
    main()








