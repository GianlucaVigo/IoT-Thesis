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
        geo_map(count_df)

    return



''''''
def geo_map(country_df):

    fig = px.choropleth(
        country_df,
        locations="country",
        color="count",
        hover_name="country",
        projection="natural earth",
        locationmode="country names" 
    )

    fig.show()

    return



''''''
def timeline(data, mode):

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

    timeline_data = {'Date': [],
                    'Value': []}

    selected_item = None

    for i, date in enumerate(dir_list):
        
        # Append to the "Date" row the dataset date: [2025-07-31].[csv] -> 2025-07-31
        timeline_data["Date"].append(date.split('.')[0])
        
        path.update({'date': date})

        '''DATA CONVERSION'''
        # CSV -> pandas dataframe
        data_df = pd.read_csv(files_handling.path_dict_to_str(path))

        '''DATA FILTERING'''
        # consider only user requested column + delete rows with NaN
        data_df = data_df.loc[:, mode].dropna(how='any', axis=0)

        if i == 0: # first iteration
            print("Here's the list of available countries collected in the first experiment date: \n")
            for i, option in enumerate(data_df.drop_duplicates()):
                print(f"{i:2d}. {option}")

            print("\nWrite the country you're interested in:")
            selected_item_id = int(input())
            selected_item = data_df.drop_duplicates().iloc[selected_item_id]

        # Append to the "Value" row the number of occurrences of the selected country
        try:
            value = int(data_df.value_counts()[selected_item])
        except:
            value = 0
        
        timeline_data["Value"].append(value)
    

    # dict to dataframe
    timeline_data_df = pd.DataFrame(timeline_data)
    timeline_data_df["Date"] = timeline_data_df["Date"].astype(str)

    # Plot horizontal bar chart
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=timeline_data_df, y="Date", x="Value", color="lightgreen")

    # Add labels on each bar
    for p in ax.patches:
        ax.text(p.get_width(),                # x-position (end of bar)
                p.get_y() + p.get_height()/2, # y-position (center of bar)
                int(p.get_width()),           # label text
                ha="left", va="center")

    plt.xlabel("Active CoAP servers")
    plt.ylabel("Dates")
    plt.title(f"<{selected_item}> active servers over time")
    plt.tight_layout()
    plt.show()



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

    stability_geo_map(data)

    return



''''''
def stability_geo_map(data):

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

    stability_df = pd.DataFrame(columns=['date', 'country', 'count'])

    for dir in dir_list:

        path.update({'date': dir})

        '''DATA CONVERSION'''
        # CSV -> pandas dataframe
        data_df = pd.read_csv(files_handling.path_dict_to_str(path))

        '''DATA FILTERING'''
        # consider only 'isCoAP' column
        for index, value in data_df.loc[:, 'country'].value_counts().items():
            new_row = pd.DataFrame({"date": [dir], "country": [index], "count": [value]})
            stability_df = pd.concat([stability_df, new_row], ignore_index=True)

    stability_df["count"] = stability_df["count"].astype(float)

    fig = px.choropleth(
        stability_df,
        locations="country",
        color="count",
        hover_name="country",
        animation_frame="date",
        animation_group="date",
        projection="natural earth",
        locationmode="country names" 
    )

    fig.show()
    
    return