import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import plotly.express as px
import gc
import datetime

from utils import files_handling
from collections import Counter


def parse_csv(data_paths, mode, ip_info_df, focused_analysis):

    # DATA
    data_paths_str = files_handling.path_dict_to_str(data_paths)
    
    # consider only the IP addresses
    data_df = pd.read_csv(data_paths_str, usecols=['saddr'])

    # data dict to plot
    dict_plot = Counter()

    for index, data_row in data_df.iterrows():

        if focused_analysis != None:
            df_to_analyse = ip_info_df[(ip_info_df['saddr'] == data_row['saddr']) & (ip_info_df[mode] == focused_analysis)]
        else:
            df_to_analyse = ip_info_df[ip_info_df['saddr'] == data_row['saddr']]

        for index, ip_info_row in df_to_analyse.iterrows():

            # format already present in the counter
            if ip_info_row[mode] in dict_plot.keys():
                dict_plot[ip_info_row[mode]] += 1
            else:
                dict_plot[ip_info_row[mode]] = 1

    # clean the memory
    del ip_info_df
    del data_df
    gc.collect()

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
def instant_analysis(data_paths, mode):

    # IP INFO DATAFRAME
    ip_info_dataset = {'phase': 'O3_IpInfo', 'folder': 'csv', 'dataset': data_paths['dataset']}
    ip_info_dataset_str = files_handling.path_dict_to_str(ip_info_dataset) + '.csv'

    # considering only necessary columns
    ip_info_df = pd.read_csv(ip_info_dataset_str, usecols=['saddr', mode])

    #######################################

    # COMPLETE or PARTIAL ANALYSIS
    focused_analysis = complete_or_partial_analysis(mode, ip_info_df)

    #######################################
    
    # MODE DISTRIBUTION DICTIONARY
    dict_plot = parse_csv(data_paths, mode, ip_info_df, focused_analysis)

    #######################################

    # DICTIONARY CONVERTION
    # Converting the dictionary into DataFrame
    df_plot = pd.DataFrame(dict_plot.items(), columns = [mode, 'count'])
    df_plot = df_plot.sort_values('count', ascending=False)
    print(f"Total Count: {df_plot['count'].sum()}\n")
    print(df_plot)

    #######################################

    # PLOTTING
    # Plot horizontal bar chart
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=df_plot, y=mode, x='count', color="lightgreen")

    # Add labels on each bar
    for p in ax.patches:
        ax.text(p.get_width(),                # x-position (end of bar)
                p.get_y() + p.get_height()/2, # y-position (center of bar)
                int(p.get_width()),           # label text
                ha="left", va="center")

    plt.xlabel("Number of associated IPs")
    plt.ylabel(f"{mode.capitalize()}")
    if focused_analysis != None:
        plt.title(f"[IP] {focused_analysis.capitalize()} Distribution")
    else:
        plt.title(f"[IP] {mode.capitalize()} Distribution")
    plt.tight_layout()
    plt.show()

    # geographical representation of the country distribution
    if mode == 'country':

        fig = px.choropleth(
            df_plot,
            locations="country",
            color="count",
            hover_name="country",
            projection="natural earth",
            locationmode="country names" 
        )

        fig.show()

    # clean the memory
    del df_plot
    gc.collect()

    return


''''''
def time_analysis(data_paths, mode):

    # IP INFO DATAFRAME
    ip_info_dataset = {'phase': 'O3_IpInfo', 'folder': 'csv', 'dataset': data_paths['dataset']}
    ip_info_dataset_str = files_handling.path_dict_to_str(ip_info_dataset) + '.csv'

    # considering only necessary columns
    ip_info_df = pd.read_csv(ip_info_dataset_str, usecols=['saddr', mode])

    #######################################

    # COMPLETE or PARTIAL ANALYSIS
    focused_analysis = complete_or_partial_analysis(mode, ip_info_df)

    #######################################

    # MODE DISTRIBUTION DATAFRAME
    # dataframes to concatenate
    df_frames = []

    # available dates
    dates = data_paths['date']

    for date in dates:
        csv_path = {'phase': data_paths['phase'], 
                    'folder': data_paths['folder'],
                    'dataset': data_paths['dataset'],
                    'date': date}
        
        # get the counter dictionary considering the selected csv and mode
        date_dict = parse_csv(csv_path, mode, ip_info_df, focused_analysis)
        # converting it to a dataframe with columns ('mode' and 'count')
        date_df = pd.DataFrame(date_dict.items(), columns = [mode, 'count'])
        # sort by count
        date_df = date_df.sort_values('count', ascending=False)
        # take only date and ignore '.csv' part of the string
        current_date = csv_path['date'].split('.')[0]
        # adding column date
        date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')
        

        # print additional info
        print('+' * 50)
        print(f"{current_date} @ Total count: {date_df['count'].sum()}\n")
        print(date_df)
        
        # parse a single dataset which has been not partitioned
        df_frames.append(date_df)

    # concatenate all dates
    df_plot = pd.concat(df_frames, ignore_index=True)

    #######################################

    # PLOTTING

    # OPTION 1
    plt.figure(figsize=(10,6))
    sns.lineplot(data=df_plot, x="date", y="count", hue=mode, marker="o")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45)
    if focused_analysis != None:
        plt.title(f"{focused_analysis.capitalize()} Distribution over Time")
    else:
        plt.title(f"{mode.capitalize()} Distribution over Time")
    plt.show()

    # OPTION 2
    plt.figure(figsize=(10,6))
    sns.barplot(data=df_plot, x=mode, y="count", hue="date")
    plt.xticks(rotation=45)
    if focused_analysis != None:
        plt.title(f"{focused_analysis.capitalize()} Distribution over Time")
    else:
        plt.title(f"{mode.capitalize()} Distribution over Time")
    plt.show()

    # OPTION 3
    heatmap_data = df_plot.pivot(index=mode, columns="date", values="count").fillna(0)
    plt.figure(figsize=(8,6))
    sns.heatmap(heatmap_data, annot=True, fmt=".0f", cmap="Blues")
    if focused_analysis != None:
        plt.title(f"Counts Heatmap: {focused_analysis.capitalize()}")
    else:
        plt.title(f"Counts Heatmap: {mode.capitalize()}")
    plt.show()

    # OPTION 4 - NB: only for 'country' option
    if mode == 'country':

        fig = px.choropleth(
            df_plot,
            locations="country",        # column with country names
            locationmode="country names",
            color="count",              # value to color by
            animation_frame="date",     # adds a time slider
            title="Counts per Country over Time"
        )
        fig.show()

        fig = px.scatter_geo(
            df_plot,
            locations="country",
            locationmode="country names",
            size="count",               # bubble size = count
            color="country",
            animation_frame="date",
            projection="natural earth",
            title="Counts per Country over Time"
        )
        fig.show()

    # clean the memory
    del df_plot
    gc.collect()

    return