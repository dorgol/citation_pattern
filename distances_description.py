import pandas as pd
import citations
import shortest_path
from functools import reduce
import numpy as np
import plotly.express as px
import plotly.io as pio
from plotly import graph_objects as go
pio.renderers.default = "browser"

path = "C:/Users/dorgo/Documents/university/thesis/new_thesis/data/"
affiliations = pd.read_csv(path + 'affiliations/' + 'affiliations.csv')

def rel_data_year(year):
    #Goal: get data of citations, paths and paths lengths of a given year in one dataframe
    #Gets: year: int, the desired year.
    #Returns: dataframe, see goal.
    df_citations = citations.reading_yearly_data(year, 'citations')
    df_paths = citations.reading_data(year, paths=True)
    years_range = list(range(1990, year + 1))
    for i in range(len(df_paths[1])):
        df_citations.insert(len(df_citations.columns), 'paths' + str(years_range[i]), df_paths[1][i])
    for i in range(len(df_paths[0])):
        df_citations.insert(len(df_citations.columns), 'lengths' + str(years_range[i]), df_paths[0][i])
    return df_citations

def get_full_data(start_year = 1990, end_year = 1992):
    #Goal: get data of citations, paths and paths lengths of every year in one dataframe
    #Gets: start_year: int, the first year in the data.
    #      end_year: int, the last year in the data.
    #Returns: dict: dictionary. Dictionaty of dataframes. Each df contains edges and paths info in one year
    years_range = range(start_year, end_year+1)
    frames = {}
    for year in years_range:
        df = rel_data_year(year)
        df1 = df.filter(regex=("Cit|length|path"))
        frames['df' +str(year)] = df1

    dict = {}
    for year in years_range:
        yearly_df = {}
        for i in range(len(frames)):
            frame = frames[list(frames.keys())[i]]
            reg = "Cit|" + str(year)
            df2 = frame.filter(regex=reg)
            yearly_df['year_' + list(frames.keys())[i]] = df2
            print(len(df2))
        df3 = pd.concat(yearly_df, axis=0)
        df3 = df3.dropna()
        df3 = df3.drop_duplicates(['CitingAffiliatoinId', 'CitedAffiliatoinId'])
        dict['df' + str(year)] = df3
    full_data = reduce(lambda left, right: pd.merge(left, right, on=['CitedAffiliatoinId', 'CitingAffiliatoinId'],
                                            how='outer'), dict.values())

    return full_data

def pdf_cdf(df, year, bins = 10):
    df = df[df.variable.str.contains(str(year))]
    series_len = df.value.values
    count, bins_count = np.histogram(series_len, bins=bins)

    # finding the PDF of the histogram using count values
    pdf = count / sum(count)

    # using numpy np.cumsum to calculate the CDF
    # We can also find using the PDF values by looping and adding
    cdf = np.cumsum(pdf)
    return cdf, pdf, bins_count

def pdf_cdf_all_years(df, bins = 100):
    years_range = range(1990,2000)
    cdfs = []
    pdfs = []
    bins_counts = []
    for year in years_range:
        tmp = pdf_cdf(df=df, year=year, bins=bins)
        cdfs.append(tmp[0])
        pdfs.append(tmp[1])
        bins_counts.append(tmp[2])
    return pdfs, cdfs, bins_counts

def plot_pcdf(data, type_dist, save = False, name = None):
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
            'text': type_dist + ' of travel times',
            'y': 0.9,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title="travel minutes",
        yaxis_title="density",
        legend_title="years",
        font=dict(
            family="Courier New, monospace",
            size=18,
            color="RebeccaPurple"
        )
    )
    fig.show()
    if save == True:
        fig.write_html('C:/Users/dorgo/Documents/university/thesis/new_thesis/plots/' + name + '.html')

def arrange_data(type_df):
    #Goal: one function to get the data in the desirable format
    #Gets: type_df: str, 'wide' or 'long'
    #Returns: if 'type_df' is set to 'wide' the function returns the data in a wide format.
    #         if 'type_df' is set to 'long' the function returns 2 dataframes - lengths and paths.
    data_wide = get_full_data(end_year=1999)
    if type_df == 'wide':
        return data_wide
    data_long = pd.melt(data_wide, id_vars=['CitingAffiliatoinId', 'CitedAffiliatoinId'])
    if type_df == 'long':
        lengths_df = data_long.loc[data_long.variable.str.contains('length')]
        paths_df = data_long.loc[data_long.variable.str.contains('path')]
        paths_df.reset_index(drop = True, inplace = True)
        lengths_df.reset_index(drop=True, inplace=True)
        return paths_df, lengths_df

def count_paths(df, table_format, year = None):
    #Goal: count the number of edges in a path
    #Gets: df, a dataframe. Either wide dataframe or paths dataframe.
    #      table_format, str. 'wide' for wide format, 'long' for long format.
    #      year: int. for 'wide' only. The year to count.
    #Returns: dataframe with count of edges.
    if table_format == 'wide':
        d = df['paths' + str(year)]
    elif table_format == 'long':
        d = df['value']
    else:
        raise Exception('table_format should take either "wide" or "long", not: {}'.format(table_format))
    paths_lengths = []
    for i in range(len(d)):
        try:
            path_length = len(d[i][0])
        except:
            path_length = 'Not Available'
        paths_lengths.append(path_length)
    paths_lengths = pd.Series(paths_lengths)
    if table_format == 'long':
        df['paths_lengths'] = paths_lengths
        paths_lengths = df
    return paths_lengths

def diff_paths(data, base_year, compared_year, to_plot = False):
    #Goal: supplying the difference in path length between years.
    #Gets: data, dataframe. Dataframe in 'wide' format.
    #      'base_year': int, the base year.
    #      'compared_year': int, the year to compare to.
    #      'to_plot': boolean, whether to plot the data or not. Default to False.
    #Returns: diff, vector. the difference in paths lengths between given years.
    #         diff_des: the .describe() of the vector.
    #         plot - histogram if 'to_plot' is set to True.
    base_path = count_paths(data, table_format='wide', year=base_year)
    compared_path = count_paths(data, table_format='wide', year=compared_year)
    a = base_path != 'Not Available'
    b = compared_path != 'Not Available'
    c = a&b
    diff = compared_path[c] - base_path[c]
    diff = diff.dropna()
    diff = diff.astype('int')
    diff_des = diff.describe()
    if to_plot:
        fig = px.histogram(diff)
        fig.show()
    return diff, diff_des

def diff_all(data, table_format):
    if table_format == 'wide':
        paths_change = []
        year = 1990
        while year < 1999:
            following_year = year + 1
            tmp = diff_paths(data=data, base_year=year, compared_year=following_year)
            tmp = pd.DataFrame(tmp[0])
            tmp['year'] = str(year) + '_' + str(following_year)
            paths_change.append(tmp)
            year += 1
        change_ways = pd.concat(paths_change)
        return change_ways

def plot_paths(data, grouping_var, outcome_var, save, name):

    fig = px.histogram(data, x=outcome_var,
                       facet_col=grouping_var, facet_col_wrap=2)
    fig.show()
    if save == True:
        fig.write_html('C:/Users/dorgo/Documents/university/thesis/new_thesis/plots/' + name + '.html')

def diff_lengths(df, save, name, pandas_stat = pd.DataFrame.shift, type_plot = None):
    years_range = range(1990,1999)
    b = df.dropna()
    b['new'] = getattr(b.groupby(['CitingAffiliatoinId', 'CitedAffiliatoinId'])['value'], pandas_stat.__name__)()
    b = b.dropna()
    def local_plot(df_plot, base_year, compared_year, save = save, name = name, type_plot1 = type_plot):

        if type_plot1 == 'density_map':
            fig = px.density_heatmap(df_plot, x="value", y="new", histfunc="avg", nbinsx=50, nbinsy=50,
                                     marginal_x="histogram", marginal_y="histogram")
        elif type_plot1 == 'scatter':
            fig = df_plot.plot.scatter(x="value", y="new")
        else:
            ValueError('type_plot must be "density_map" or "scatter"')
        fig.update_layout(
            title="Distances between Years",
            xaxis_title="distances in year " + str(base_year),
            yaxis_title="distances in year " + str(compared_year),
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )

        fig.update_layout(
            title={
                'y': 1,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'})


        if save == True:
            fig.write_html('C:/Users/dorgo/Documents/university/thesis/new_thesis/plots/' +
                           name + str(base_year) + '_' + str(compared_year) + '.html')
        fig.show()
    pd.options.plotting.backend = 'plotly'
    from plotly.subplots import make_subplots
    fig = make_subplots(rows=1, cols=2)
    for i in years_range:
        if i == 1999:
            break
        reg = str(i) + '|' + str(i + 1)
        c = b[b['variable'].str.contains(reg)]
        local_plot(df_plot=c, save = save,name = name, base_year = i,compared_year= i+1, type_plot1 = type_plot)

def main():
    #get data
    data_wide = arrange_data(type_df='wide')
    paths_df, lengths_df = arrange_data(type_df='long')
    all_pcdf = pdf_cdf_all_years(lengths_df)
    count_paths(paths_df, 'long')
    plot_pcdf(all_pcdf, 'cdf', True, 'cdf_lengths')
    plot_pcdf(all_pcdf, 'pdf', True, 'pdf_lengths')
    a = diff_all(data_wide, table_format = 'wide')
    plot_paths(a, grouping_var = 'year', outcome_var = 0, save=True, name='paths')
    diff_lengths(df=lengths_df, save=True, name='distances', type_plot='scatter')
    diff_lengths(df=lengths_df, save=True, name='distances_density', type_plot='density_map')

if __name__ == '__main__':
    main()