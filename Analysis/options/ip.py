import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import os
import plotly.express as px

from utils import files_handling


''''''
def instant_analysis(data, mode):

    count_s = data.groupby([mode]).size()
    count_df = pd.DataFrame(count_s).reset_index()
    count_df = count_df.rename(columns={0: 'count'})

    # Plot horizontal bar chart
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=count_df.sort_values('count'), y=mode, x="count", color="lightgreen")

    # Add labels on each bar
    for p in ax.patches:
        ax.text(p.get_width(),                # x-position (end of bar)
                p.get_y() + p.get_height()/2, # y-position (center of bar)
                int(p.get_width()),           # label text
                ha="left", va="center")

    plt.xlabel("Number of associated IPs")
    plt.ylabel(f"{mode.capitalize()}")
    plt.title(f"[IP] {mode.capitalize()} Distribution")
    plt.tight_layout()
    plt.show()

    if mode == 'country':
        # geographical representation of the country distribution
        fig = px.choropleth(
        count_df,
        locations="country",
        color="count",
        hover_name="country",
        projection="natural earth",
        locationmode="country names" 
    )

    fig.show()

    return


''''''
def time_analysis(data_df, mode):

    # easier to read
    print(data_df.groupby(['Date', mode]).size().to_frame('count'))

    df_plot = data_df.value_counts(subset=['Date', mode]).reset_index(name='count')

    # OPTION 1
    plt.figure(figsize=(10,6))
    sns.lineplot(data=df_plot, x="Date", y="count", hue=mode, marker="o")
    plt.xticks(rotation=45)
    plt.title(f"Counts per {mode.capitalize()} over Time")
    plt.show()

    # OPTION 2
    plt.figure(figsize=(10,6))
    sns.barplot(data=df_plot, x="Date", y="count", hue=mode)
    plt.xticks(rotation=45)
    plt.title(f"Counts per {mode.capitalize()} per Date")
    plt.show()

    # OPTION 3
    heatmap_data = df_plot.pivot(index=mode, columns="Date", values="count").fillna(0)
    plt.figure(figsize=(8,6))
    sns.heatmap(heatmap_data, annot=True, fmt=".0f", cmap="Blues")
    plt.title(f"Counts Heatmap: {mode.capitalize()}")
    plt.show()

    # OPTION 4 - NB: only for 'country' option
    if mode == 'country':

        fig = px.choropleth(
            df_plot,
            locations="country",        # column with country names
            locationmode="country names",
            color="count",              # value to color by
            animation_frame="Date",     # adds a time slider
            title="Counts per Country over Time"
        )
        fig.show()

        fig = px.scatter_geo(
            df_plot,
            locations="country",
            locationmode="country names",
            size="count",               # bubble size = count
            color="country",
            animation_frame="Date",
            projection="natural earth",
            title="Counts per Country over Time"
        )
        fig.show()


''''''
def stability(data):

    print("[IP] IP addresses stability over time")

    # Before: DataRefinement/results/02_output.csv/exp_0/2025-07-31.csv
    path = {
        'phase': data['phase'],
        'folder': data['folder'],
        'ZMAP dataset': data['ZMAP dataset'],
        'experiment': data['experiment']
    }
    #  After: DataRefinement/results/02_output.csv/exp_0/

    dir_list = os.listdir(files_handling.path_dict_to_str(path))
    dir_list.sort()

    if (len(dir_list) <= 1):
            print("There must be at least 2 datasets to examine the stability property: please acquire more data!")
            print("-" * 100)
            return 

    stability_df = pd.DataFrame()

    for dir in dir_list:

        path.update({'date': dir})

        '''DATA CONVERSION'''
        # CSV -> pandas dataframe
        data_df = pd.read_csv(files_handling.path_dict_to_str(path))

        '''DATA FILTERING'''
        # consider only 'isCoAP' column
        data_series = data_df.loc[:, 'isCoAP']

        stability_df[dir] = data_series

    stability_df = stability_df.reindex(sorted(stability_df.columns), axis=1)
    
    sns.heatmap(stability_df, fmt="d", cmap="YlGnBu")
    plt.show()

    clean_stability_df = pd.DataFrame()

    for i, row in stability_df.iterrows():
        if i == 0:
            continue
        for i in range(1, len(row)):
             if(row.iloc[i] == row.iloc[i-1]): # HERE!
                continue
             else:
                clean_stability_df = pd.concat([clean_stability_df, pd.DataFrame([row])], ignore_index=True)
                break

    sns.heatmap(clean_stability_df, fmt="d", cmap="YlGnBu")
    plt.show()

    '''STATS'''
    print(f"\tNumber of instable ip addresses: {clean_stability_df.shape[0]}")
    print(f"\tNumber of stable ip addresses: {stability_df.shape[0] - clean_stability_df.shape[0]}")
    print("-" * 100)

    return