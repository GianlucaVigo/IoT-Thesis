import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import plotly.express as px
import gc
import datetime

from utils import files_handling
from collections import Counter

CHUNK_SIZE = 10000

def parse_csv(data_path, mode, ip_info_df, focused_analysis):

    # data dict to plot
    dict_plot = Counter()

    with pd.read_csv(data_path, chunksize=CHUNK_SIZE, usecols=['saddr']) as csv_reader:

        for chunk in csv_reader:

            for index, data_row in chunk.iterrows():

                if focused_analysis != None:
                    df_to_analyse = ip_info_df[(ip_info_df['saddr'] == data_row['saddr']) & (ip_info_df[mode] == focused_analysis)]
                else:
                    df_to_analyse = ip_info_df[ip_info_df['saddr'] == data_row['saddr']]

                for index, ip_info_row in df_to_analyse.iterrows():

                    # format already present in the counter
                    dict_plot.update([ip_info_row[mode]])

            # clean the memory
            del chunk

            
        # clean the memory
        del ip_info_df
        gc.collect()

    print(dict_plot)

    return dict_plot


def complete_or_partial_analysis(mode, ip_info_df):

    # focus on single element or entire picture?
    print(f"Do you want to analyse data of:\n\t1. one specific {mode}\n\t2. all of them\n")
    focused_analysis_choice = int(input())

    focused_analysis = None

    if focused_analysis_choice == 1:
        print(f"Select the {mode} you're interested:\n")
        elements = ip_info_df[mode].unique()
        for id, elem in enumerate(elements):
            print(f"\t{id}. {elem}")
    
        print("\nPlease, specify the index:")
        choice = int(input())

        print(f"You have selected: {elements[choice]}")

        focused_analysis = elements[choice]

    return focused_analysis


''''''
def analysis(paths, mode):

    # IP INFO DATAFRAME
    ip_info_df_frames = []

    for path_dict in paths:

        with pd.read_csv(path_dict['ip_info'], chunksize=CHUNK_SIZE, usecols=['saddr', mode]) as csv_reader:

            for chunk in csv_reader:

                ip_info_df_frames.append(chunk)

    ip_info_df = pd.concat(ip_info_df_frames, ignore_index=True)

    #######################################

    # COMPLETE or PARTIAL ANALYSIS
    focused_analysis = complete_or_partial_analysis(mode, ip_info_df)

    #######################################

    # MODE DISTRIBUTION DATAFRAME
    # dataframes to concatenate
    data_per_date_dict = []

    # internet partition level
    for path_dict in paths:

        # dates level
        for date_path in path_dict['data']:

            # take only date and ignore '.csv' part of the string
            current_date = date_path.split('/')[5][:-4]
        
            # get the counter dictionary considering the selected csv partition and mode
            date_dict = parse_csv(date_path, mode, ip_info_df, focused_analysis)

            to_store = {'date': current_date, 'data': date_dict}

            appended = False 
            for date in data_per_date_dict:
                if date['date'] == to_store['date']:
                    date['data'].update(to_store['data'])
                    appended = True

            if not appended:
                data_per_date_dict.append(to_store)

    # flatten the structure
    rows = [
        {"date": entry["date"], mode: country, "count": count}
        for entry in data_per_date_dict
        for country, count in entry["data"].items()
    ]

    # convert the list of dictionaries into a pandas dataframe for plotting
    df_plot = pd.DataFrame(rows)
    df_plot.sort_values(by=['date', 'count'], ignore_index=True, inplace=True, ascending=True)

    print(df_plot)

   #######################################

    # PLOTTING

    # OPTION 1
    fig = px.line(
        df_plot, 
        x="date", 
        y="count", 
        title=f'{mode} distribution',
        color=mode
    )
    fig.show()

    # OPTION 2
    heatmap_data = df_plot.pivot(index=mode, columns="date", values="count").fillna(0)
    plt.figure(figsize=(8,6))
    sns.heatmap(heatmap_data, annot=True, fmt=".0f", cmap="Blues")
    if focused_analysis != None:
        plt.title(f"Counts Heatmap: {focused_analysis.capitalize()}")
    else:
        plt.title(f"Counts Heatmap: {mode.capitalize()}")
    plt.show()

    # OPTION 3 - NB: only for 'country' option
    if mode == 'country':

        fig = px.choropleth(
            df_plot,
            locations="country",        # column with country names
            locationmode="country names",
            color="count",              # value to color by
            animation_frame="date",     # adds a time slider
            projection="natural earth",
            title="Counts per Country over Time"
        )
        fig.show()

    # clean the memory
    del df_plot
    gc.collect()

    return