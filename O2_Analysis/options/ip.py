import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import gc
import datetime

from collections import Counter

#############################

CHUNK_SIZE = 10000

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
        current_date = path.split('/')[4][:-4] 
            
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
    
    unique_elements = df_plot[mode].unique()

    #######################################

    # COMPLETE or PARTIAL ANALYSIS
    focused_analysis = complete_or_partial_analysis(mode, unique_elements)

    #######################################
    
    if focused_analysis != None:
        df_plot = df_plot[df_plot[mode] == focused_analysis]
        
    df_plot.sort_values(by=['date', 'count'], ignore_index=True, inplace=True, ascending=True)
    print(df_plot)

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

#############################

def stability_analysis(data_paths):
    
    on = Counter()
    off = Counter()
    
    
    portions_datasets = {}
    
    for path in data_paths:
        portion_id = int(path.split('/')[3])
        
        if portion_id in portions_datasets.keys():
            portions_datasets[portion_id].append(path)
        else:
            portions_datasets[portion_id] = [path]

    
    
    ip_stability = {}
    
    for _, portion_dataset_path in portions_datasets.items():
        
        for date_path in portion_dataset_path:
            
            current_date_str = date_path.split('/')[4][:-4]
        
            for chunk in pd.read_csv(date_path, chunksize=CHUNK_SIZE, usecols=['saddr', 'code', 'classification']):
                
                # DATA CLEANING
                # keeping only entries having classification equal to udp
                chunk = chunk[chunk['classification'] == 'udp']
                # deleting rows with code field equal to nan
                chunk.dropna(ignore_index=True, inplace=True, subset=['code'])
                
                for _, row in chunk.iterrows():
                    
                    ip = row['saddr']
                    
                    # ip address has been already seen
                    if ip in ip_stability.keys():
                        
                        modified_last_str = ip_stability[ip][1]
                        
                        current_date = datetime.datetime.strptime(current_date_str, "%Y-%m-%d").date()
                        modified_last = datetime.datetime.strptime(modified_last_str, "%Y-%m-%d").date()
                        
                        diff = (current_date - modified_last).days
                        
                        # yesterday was still active/on
                        if diff == 1:
                            
                            # incrementing the time interval in which the ip is on/active
                            ip_stability[ip][0] += 1
                            # updating modified last date
                            ip_stability[ip][1] = current_date_str
                        
                        # from diff days the ip was not on/active
                        elif diff > 1:
                            
                            on.update([ip_stability[ip][0]])
                            off.update([diff])
                            
                            # start over
                            ip_stability[ip] = [1, current_date_str]
                            
                    # first time an ip address appears
                    else:
                        ip_stability[ip] = [1, current_date_str]
    
    today = datetime.date.today()
    
    for ip in ip_stability.keys():
        
        last_date_str = ip_stability[ip][1]
        last_date = datetime.datetime.strptime(last_date_str, "%Y-%m-%d").date()
        
        on.update([ip_stability[ip][0]])
        
        if last_date == today:
            continue
        
        diff = (today - last_date).days
        off.update([diff])
    
    ##############################
    
    # DICTIONARY -> DATAFRAME
    on_plot = pd.DataFrame(on.items(), columns=['days_on', 'instances'])
    on_plot = on_plot.sort_values('days_on', ascending=True).reset_index(drop=True)
    
    off_plot = pd.DataFrame(off.items(), columns=['days_off', 'instances'])
    off_plot = off_plot.sort_values('days_off', ascending=True).reset_index(drop=True)
    
    print(on_plot)
    print(off_plot)

    ##############################

    # PLOTTING
    fig = px.bar(
        on_plot,
        x='days_on',
        y='instances',
        title="CoAP Servers' stability"
    )

    fig.show()
    
    fig = px.bar(
        off_plot,
        x='days_off',
        y='instances',
        title="Inactivity Periods"
    )
    
    fig.show()
    
    return