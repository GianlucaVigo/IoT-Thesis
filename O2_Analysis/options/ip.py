import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import gc

from collections import Counter

#############################

CHUNK_SIZE = 1000

#############################

def complete_or_partial_analysis(mode, unique_elements):

    # focus on single element or entire picture?
    print(f"Do you want to analyse data of:\n\t1. one specific {mode}\n\t2. all of them\n")
    focused_analysis_choice = int(input())

    focused_analysis = None

    if focused_analysis_choice == 1:
        print(f"Select the {mode} you're interested:\n")

        for id, elem in enumerate(unique_elements):
            print(f"\t{id}. {elem}")
    
        print("\nPlease, specify the index:")
        choice = int(input())

        print(f"You have selected: {unique_elements[choice]}")

        focused_analysis = unique_elements[choice]

    return focused_analysis

#############################

def analysis(paths, mode):
    
    data_per_date_dict = {} 
        
    for path in paths:
            
        # take only date and ignore '.csv' part of the string 
        current_date = path.split('/')[5][:10]
            
        data_dict = Counter() 
            
        for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=[mode]): 
                
            vc = chunk[mode].value_counts(dropna=True)
                
            data_dict.update(vc.to_dict()) 
            
            
        if current_date in data_per_date_dict:
            data_per_date_dict[current_date].update(data_dict)
        else:
            data_per_date_dict[current_date] = data_dict 
                
    # flatten the structure 
    rows = [ 
        {"date": date, mode: item, "count": count} 
        for date, counter in data_per_date_dict.items() 
        for item, count in counter.items() 
    ] 
            
    df_plot = pd.DataFrame(rows)
    
    #######################################
    
    # extract a list of unique elements per 'mode'
    unique_elements = df_plot[mode].unique()

    # COMPLETE or PARTIAL ANALYSIS
    focused_analysis = complete_or_partial_analysis(mode, unique_elements)

    #######################################
    
    if focused_analysis != None:
        df_plot = df_plot[df_plot[mode] == focused_analysis].sort_values(["date", "count"], ascending=[True, False])
    else:
        df_plot = df_plot.sort_values(["date", "count"], ascending=[True, False])
    
    
    # logging
    print(df_plot)
    
    
    # OPTION 1
    fig = px.line(
        df_plot, 
        x="date", 
        y="count", 
        title=f'{mode} distribution',
        color=mode
    )
    fig.show()
    
    
    # OPTION 2 - NB: only for 'country' option
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
        

    # OPTION 3
    plt.figure(figsize=(8,6))
    
    if focused_analysis != None:
        df_plot = df_plot.pivot(index=mode, columns="date", values="count").fillna(0)
        
        plt.title(f"Counts Heatmap: {focused_analysis.capitalize()}")
        
    else:
        df_plot = df_plot.groupby("date", as_index=False).head(30).pivot(index=mode, columns="date", values="count").fillna(0)
        
        if mode == 'continent':
            plt.title(f"Counts Heatmap: {mode.capitalize()}")
        else:
            plt.title(f"Counts Heatmap - TOP 30: {mode.capitalize()}")
        
    sns.heatmap(df_plot, annot=True, fmt=".0f", cmap="Blues")
    
    plt.show()


    # clean the memory
    del df_plot
    gc.collect()

    return

#############################

def stability_analysis(paths):
    
    data_per_date_dict = {} 
        
    for path in paths:
            
        # take only date and ignore '.csv' part of the string 
        current_date = path.split('/')[5][:10]
            
        for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['saddr']):
            
            if current_date not in data_per_date_dict.keys():
                
                data_per_date_dict[current_date] = 0
                
            data_per_date_dict[current_date] += chunk.shape[0]
            
    
    df_plot = pd.DataFrame(data_per_date_dict.items(), columns=['date', 'count'])
    
    print(df_plot)
    
    
    # OPTION 1
    fig = px.line(
        df_plot, 
        x="date", 
        y="count", 
        title=f'COAP Machines Stability'
    )
    fig.show()
    
    
    # clean the memory
    del df_plot
    gc.collect()

    return