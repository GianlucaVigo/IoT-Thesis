import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import os
import datetime
import plotly.express as px
from collections import Counter

''''''
def country(data):

    print("[IP] Country Distribution")

    sns.set_theme()

    data_df = pd.read_csv(data)

    country_df = data_df.loc[:, 'country'].dropna(how='any', axis=0)

    plot = sns.displot(data=country_df)
    plot.set_xticklabels(rotation=45)

    plt.show()

    # geographical representation of the country distribution
    country_map(country_df)

    country_timeline(data)

    print("-" * 100)
    return



''''''
def country_map(data):

    print("[IP] Geographical Country Distribution")

    geo_country_df = (data.value_counts()
                           .rename_axis('country')
                           .reset_index(name='count'))

    fig = px.choropleth(
        geo_country_df,
        locations="country",
        color="count",
        hover_name="country",
        #color_continuous_scale="Viridis",
        projection="natural earth",
        locationmode="country names" 
    )

    fig.show()

    return



''''''
def continent(data):

    print("[IP] Continent Distribution")

    sns.set_theme()

    data_df = pd.read_csv(data)

    continent_df = data_df.loc[:, 'continent'].dropna(how='any', axis=0)

    plot = sns.displot(data=continent_df)
    plot.set_xticklabels(rotation=45)

    plt.show()

    print("-" * 100)
    return



''''''
def as_name(data):

    print("[IP] AS Name Distribution")

    sns.set_theme()

    data_df = pd.read_csv(data)

    as_name_df = data_df.loc[:, 'as_name'].dropna(how='any', axis=0)

    plot = sns.displot(data=as_name_df)
    plot.set_xticklabels(rotation=90)

    plt.show()

    print("-" * 100)
    return



''''''
def country_timeline(data):
    print("[IP] Country over time")

    # Before: DataRefinement/results/02_output.csv/exp_0/2025-07-31/2025-07-31 13:47:30.325439.csv
    data = data[:-41]
    # After: DataRefinement/results/02_output.csv/exp_0/

    dir_list = os.listdir(data)
    dir_list.sort()

    country_data = {'Date': [],
                    'Value': []}

    selected_country = None

    for i, date in enumerate(dir_list):
        
        # Append to the "Date" row the dataset date
        country_data["Date"].append(date)
        
        path = f"{data}{date}/"
        dataset = os.listdir(path)
        path += dataset[0] # assuming only 1 dataset per directory

        '''DATA CONVERSION'''
        # CSV -> pandas dataframe
        data_df = pd.read_csv(path)

        '''DATA FILTERING'''
        # consider only 'isCoAP' column + delete rows with NaN
        data_df = data_df.loc[:, 'country'].dropna(how='any', axis=0)

        if i == 0: # first iteration
            print("Here's the list of available countries collected in the first experiment date: \n")
            for i, country in enumerate(data_df.drop_duplicates()):
                print(f"{i:2d}. {country}")

            print("\nWrite the country you're interested in:")
            selected_country = input().capitalize()

        # Append to the "Value" row the number of occurrences of the selected country
        value = data_df.value_counts()[selected_country]
        country_data["Value"].append(int(value))
    

    # dict to dataframe
    country_data_df = pd.DataFrame(country_data)
    country_data_df["Date"] = pd.to_datetime(country_data_df["Date"])

    # Plot horizontal bar chart
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=country_data_df, y="Date", x="Value", color="lightgreen")

    # Add labels on each bar
    for p in ax.patches:
        ax.text(p.get_width(),                # x-position (end of bar)
                p.get_y() + p.get_height()/2, # y-position (center of bar)
                int(p.get_width()),           # label text
                ha="left", va="center")

    plt.xlabel("Active CoAP servers")
    plt.ylabel("Dates")
    plt.title(f"{selected_country} active servers over time")
    plt.tight_layout()
    plt.show()



''''''
def stability(data):

    print("[IP] IP addresses stability over time")

    data = data[:-41]
    dir_list = os.listdir(data)

    if (len(dir_list) <= 1):
            print("There must be at least 2 datasets to examine the stability property: please acquire more data!")
            print("-" * 100)
            return 

    stability_df = pd.DataFrame()

    for dir in dir_list:
        path = f"{data}{dir}/"

        # assuming only 1 dataset per directory
        dataset = os.listdir(path)

        path += dataset[0]

        '''DATA CONVERSION'''
        # CSV -> pandas dataframe
        data_df = pd.read_csv(path)

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

    dir_list = os.listdir(data)
    dir_list.sort()

    if (len(dir_list) <= 1):
            print("There must be at least 2 datasets to examine the stability property: please acquire more data!")
            print("-" * 100)
            return 

    stability_df = pd.DataFrame(columns=['date', 'country', 'count'])

    for dir in dir_list:
        path = f"{data}{dir}/"

        # assuming only 1 dataset per directory
        dataset = os.listdir(path)

        path += dataset[0]

        '''DATA CONVERSION'''
        # CSV -> pandas dataframe
        data_df = pd.read_csv(path)

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
        color_continuous_scale="Viridis",
        projection="natural earth",
        locationmode="country names" 
    )

    fig.show()
    
    return