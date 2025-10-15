import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import plotly.express as px
import gc
import datetime

from utils import payload_handling
from utils import files_handling


def stability_analysis(data_path, mode):

    cols = []

    match mode:

        # 'udp_pkt_size' column
        case 'UDP Packet Size':
            cols.extend(['udp_pkt_size', 'data'])
        # 'success' column
        case 'Success':
            cols.append('success')
        # 'data' column
        case _ :
            cols.append('data')
        
    data_df = pd.read_csv(files_handling.path_dict_to_str(data_path), usecols=cols)

    # Plot Figure Size
    plt.figure(figsize=(8, 5))

    match mode:

        # 0
        case 'Payload Size':

            # DATA CLEANING
            # deleting rows with data field equal to nan
            data_df.dropna(ignore_index=True, inplace=True, subset='data')

            ##############################

            # DATA SIZE DICTIONARY
            # instantiating a Counter Dictionary
            payload_sizes_dict = Counter()

            # iterating over data_df rows
            for index, payload in data_df.iterrows():

                # getting 'data' field size
                payload_size = len(payload['data'])

                # updating the Counter Dictionary
                payload_sizes_dict.update([payload_size])

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(payload_sizes_dict.items(), columns = ['size', 'count'])
            df_plot = df_plot.sort_values('size').reset_index(drop=True)

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, x='size', y='count', color="lightgreen")
            
            # Plot Labels
            plt.xlabel('Size [Bytes]')
            plt.ylabel("# CoAP Servers | # IPs")

            # Plot Title
            plt.title("Resource Discovery Payload Size Distribution")

            ##############################

            # STATS
            print(f"Size Distribution:\n{df_plot}")


        # 1
        case 'Most Common':

            # DATA CLEANING
            # deleting rows with data field equal to nan
            data_df.dropna(ignore_index=True, inplace=True, subset='data')

            ##############################

            # URIs DICTIONARY
            # instantiating a Counter Dictionary
            uris_dict = Counter()

            # iterating over data_df rows
            for index, payload in data_df.iterrows():

                uris_list = payload_handling.uri_list_of(payload['data'])
                uris_dict.update(uris_list)

            ##############################

            # DICTIONARY TO DATAFRAME

            # getting the most common 30 URIs
            # most_common() method returns a list
            top_uris = uris_dict.most_common(30)
            
            # list -> pd DataFrame
            df_plot = pd.DataFrame(top_uris, columns=['uri', 'count'])

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, y="uri", x="count", color="lightgreen")

            # Plot Labels
            plt.xlabel('Count')
            plt.ylabel("URI")

            # Plot Title
            plt.title("[PAYLOAD] TOP 30 Most Common URI")

            ##############################

            # STATS
            print(f"TOP 30 Most Common URI:\n {df_plot}")

        
        # 2
        case 'Resources Number':

            # DATA CLEANING
            # deleting rows with data field equal to nan
            data_df.dropna(ignore_index=True, inplace=True, subset='data')

            ##############################

            # NUM of RESOURCEs DICTIONARY
            # instantiating a Counter Dictionary
            n_resources_dict = Counter()
            
            # iterating over data_df rows
            for index, payload in data_df.iterrows():

                n_resources_list = len(payload_handling.resource_list_of(payload['data']))
                n_resources_dict.update([n_resources_list])

            ##############################

            # DICTIONARY TO DATAFRAME
            df_plot = pd.DataFrame(n_resources_dict.items(), columns=['n_resources', 'count'])
            df_plot = df_plot.sort_values('n_resources').reset_index(drop=True)

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, x='n_resources', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Number of Resources')
            plt.ylabel("# CoAP Machines")

            # Plot Title
            plt.title("[PAYLOAD] Number of Resources per CoAP Machine")

            ##############################

            # STATS
            print(f"Number of Resources per CoAP Machine:\n {df_plot}\n")
            print(f"Total Number of resources:\n {(df_plot['n_resources'] * df_plot['count']).sum()}\n")


        # 3
        case 'Resource URI Depth':

            # DATA CLEANING
            # deleting rows with data field equal to nan
            data_df.dropna(ignore_index=True, inplace=True, subset='data')

            ##############################

            # NUM of LEVELS DICTIONARY
            n_levels_dict = Counter()

            # iterating over data_df rows
            for index, payload in data_df.iterrows():

                uris_list = payload_handling.uri_list_of(payload['data'])

                n_levels_list = []
                for uri in uris_list:
                    n_levels_list.append(payload_handling.n_levels_of(uri))

                n_levels_dict.update(n_levels_list)

            ##############################

            # DICTIONARY TO DATAFRAME
            df_plot = pd.DataFrame(n_levels_dict.items(), columns=['n_levels', 'count'])
            df_plot = df_plot.sort_values('n_levels').reset_index(drop=True)

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, x='n_levels', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Number of Levels')
            plt.ylabel("# CoAP Resources")

            # Plot Title
            plt.title("[PAYLOAD] Number of Levels per CoAP Resource")

            ##############################

            # STATS
            print(f"Resource URI depth:\n {df_plot}")


        # 4
        case 'Active CoAP Machines':

            # IP INFO DATAFRAME
            ip_info_dataset = {'phase': 'O3_IpInfo', 'folder': 'csv', 'dataset': data_path['dataset']}
            ip_info_dataset_str = files_handling.path_dict_to_str(ip_info_dataset) + '.csv'

            # considering only necessary columns
            ip_info_df = pd.read_csv(ip_info_dataset_str, usecols=['saddr'])

            #######################################

            # IN/ACTIVE DICTIONARY
            in_active_dict = {'active': data_df.shape[0], 'inactive': ip_info_df.shape[0] - data_df.shape[0]}

            ##############################

            # DICTIONARY TO DATAFRAME
            df_plot = pd.DataFrame(in_active_dict.items(), columns=['in/active', 'count'])

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, x='in/active', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Active/Inactive CoAP Machine')
            plt.ylabel("# CoAP Machines")

            # Plot Title
            plt.title('[PAYLOAD] Number of active/inactive CoAP machines')

            ##############################

            # STATS
            print(f"Active/Inactive CoAP machines:\n {df_plot}")

        
        # 5
        case '/.well-known/core Visibility':

            # DATA CLEANING
            # deleting rows with data field equal to nan
            data_df.dropna(ignore_index=True, inplace=True, subset='data')

            ##############################

            # /.well-known/core RESOURCE DICTIONARY
            # instantiating a Counter Dictionary
            resources_dict = {'</.well-known/core>': 0, 'n_coap_servers': 0}
            
            # iterating over data_df rows
            for index, payload in data_df.iterrows():

                # updating total number of coap servers
                resources_dict['n_coap_servers'] += 1

                uris = payload_handling.uri_list_of(payload['data'])

                if '</.well-known/core>' in uris:
                    resources_dict['</.well-known/core>'] += 1

            ##############################

            # DICTIONARY TO DATAFRAME
            df_plot = pd.DataFrame(resources_dict.items(), columns=['item', 'count'])

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, x='item', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('/.well-known/core wrt # IP addresses')
            plt.ylabel("count")

            # Plot Title
            plt.title("[PAYLOAD] '/.well-known/core' visibility")

            ##############################

            # STATS
            print(f"'/.well-known/core' visibility:\n {df_plot}\n")

        
        # 6
        case 'Resource Metadata':

            # DATA CLEANING
            # deleting rows with data field equal to nan
            data_df.dropna(ignore_index=True, inplace=True, subset='data')

            ##############################

            # RESOURCEs METADATA DICTIONARY
            metadata_dict = Counter()
            
            # iterating over data_df rows
            for index, payload in data_df.iterrows():

                resources_list = payload_handling.resource_list_of(payload['data'])

                for resource_payload in resources_list:
                    metadata_dict.update(payload_handling.resource_metadata_names_of(resource_payload))

            ##############################

            # DICTIONARY TO DATAFRAME
            df_plot = pd.DataFrame(metadata_dict.items(), columns=['metadata', 'count'])

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, x='metadata', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Resource Metadata')
            plt.ylabel("# Metadata Occurrences")

            # Plot Title
            plt.title("[PAYLOAD] Metadata Distribution over CoAP Resources")

            ##############################

            # STATS
            print(f"Metadata Distribution over CoAP Resources:\n {df_plot}")


        # 7
        case 'Content Type Metadata':

            # DATA CLEANING
            # deleting rows with data field equal to nan
            data_df.dropna(ignore_index=True, inplace=True, subset='data')

            ##############################

            # RESOURCEs CONTENT TYPE (CT) METADATA DICTIONARY
            metadata_ct_dict = Counter()

            # iterating over data_df rows
            for index, payload in data_df.iterrows():

                resources_list = payload_handling.resource_list_of(payload['data'])

                for resource in resources_list:

                    ct_value = payload_handling.get_metadata_value_of(resource, 'ct')

                    if ct_value is not None:
                        metadata_ct_dict.update(ct_value)
            
            ##############################

            # DICTIONARY TO DATAFRAME
            df_plot = pd.DataFrame(metadata_ct_dict.items(), columns=['content_type', 'count'])

            ##############################

            # PLOTTING
            # Type of plot
            sns.barplot(data=df_plot, x='content_type', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Content Type')
            plt.ylabel("# CT Occurencies")

            # Plot Title
            plt.title("[PAYLOAD] CT Metadata Distribution over CoAP Resources")

            ##############################

            # STATS
            print(f"CT Metadata Distribution over CoAP Resources:\n {df_plot}")

    '''PLOTTING'''
    plt.tight_layout()
    plt.show()

    return


def parse_csv(data_path, mode):

    cols = []

    match mode:

        # 'udp_pkt_size' column
        case 'UDP Packet Size':
            cols.extend(['udp_pkt_size', 'data'])
        # 'success' column
        case 'Success':
            cols.append('success')
        # 'data' column
        case _ :
            cols.append('data')
        
    data_df = pd.read_csv(files_handling.path_dict_to_str(data_path), usecols=cols)

    return data_df



def evolution_analysis(data_paths, mode):

    match mode:

        # 0
        case 'Payload Size':

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                date_df.dropna(ignore_index=True, inplace=True, subset='data')

                ##############################

                # DATA SIZE DICTIONARY
                # instantiating a Counter Dictionary
                payload_sizes_dict = Counter()

                # iterating over data_df rows
                for index, payload in date_df.iterrows():

                    # getting 'data' field size
                    payload_size = len(payload['data'])

                    # updating the Counter Dictionary
                    payload_sizes_dict.update([payload_size])

                ##############################

                # DICTIONARY TO DATAFRAME
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(payload_sizes_dict.items(), columns = ['size', 'count'])
                date_df = date_df.sort_values('size').reset_index(drop=True)
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print(date_df)

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)


            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)
            print(df_plot)

            ##############################

            # PLOTTING
            # opt. 1
            sns.kdeplot(data = df_plot,
                x="size",
                weights="count",
                hue = "date",
                palette = "muted"
            )
            plt.show()

            # opt. 2
            sns.kdeplot(data = df_plot,
                x="size",
                weights="count",
                hue = "date",
                palette = "muted",
                cumulative=True, 
                common_norm=False, 
                common_grid=True
            )
            plt.show()

            # opt. 3
            sns.kdeplot(
                data=df_plot, x="size", y="count", hue="date", fill=True
            )
            plt.show()
        

        # 1
        case 'Most Common':

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                date_df.dropna(ignore_index=True, inplace=True, subset='data')

                ##############################

                # URIs DICTIONARY
                # instantiating a Counter Dictionary
                uris_dict = Counter()

                # iterating over date_df rows
                for index, payload in date_df.iterrows():

                    uris_list = payload_handling.uri_list_of(payload['data'])

                    # updating the Counter Dictionary
                    uris_dict.update(uris_list)

                ##############################

                # DICTIONARY TO DATAFRAME
                top_uris = uris_dict.most_common(30)
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(top_uris, columns = ['uri', 'count'])
                date_df = date_df.sort_values('count', ascending=False).reset_index(drop=True)
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print(date_df)

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)


            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x="count",
                y="uri",
                color="uri",          # optional: color by URI
                animation_frame="date",
                orientation='h',       # horizontal bars
                title="Top 30 URIs per Date",
                text="count"           # show count on bars
            )

            fig.update_layout(yaxis={'categoryorder':'total ascending'})

            fig.show()

        
        # 2
        case 'Resources Number':

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                date_df.dropna(ignore_index=True, inplace=True, subset='data')

                ##############################

                # NUM of RESOURCEs DICTIONARY
                # instantiating a Counter Dictionary
                n_resources_dict = Counter()

                # iterating over date_df rows
                for index, payload in date_df.iterrows():
                    
                    n_resources_list = [len(payload_handling.uri_list_of(payload['data']))]
                    
                    # updating the Counter Dictionary
                    n_resources_dict.update(n_resources_list)

                ##############################

                # DICTIONARY TO DATAFRAME
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(n_resources_dict, columns = ['res_num', 'count'])
                date_df = date_df.sort_values('res_num').reset_index(drop=True)
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print('@'*50)
                print(f"\nDate {current_date}")
                print(f"Number of Resources per CoAP Machine:\n {date_df}\n")
                print(f"Total Number of resources:\n {(date_df['res_num'] * date_df['count']).sum()}\n")

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)


            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)

            ##############################

            fig = px.bar(
                df_plot,
                x="res_num",
                y="count",
                color="count",
                animation_frame="date",
                title="Resource Number per CoAP Machine",
                text="count"           # show count on bars
            )

            fig.show()
            

        # 3
        case 'Resource URI Depth':

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                date_df.dropna(ignore_index=True, inplace=True, subset='data')

                ##############################

                # NUM of LEVELs DICTIONARY
                # instantiating a Counter Dictionary
                n_levels_dict = Counter()

                # iterating over date_df rows
                for index, payload in date_df.iterrows():

                    uris_list = payload_handling.uri_list_of(payload['data'])

                    n_levels_list = []
                    for uri in uris_list:
                        n_levels_list.append(payload_handling.n_levels_of(uri))

                    # updating the Counter Dictionary
                    n_levels_dict.update(n_levels_list)

                ##############################

                # DICTIONARY TO DATAFRAME
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(n_levels_dict, columns = ['n_levels', 'count'])
                date_df = date_df.sort_values('n_levels').reset_index(drop=True)
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print('@'*50)
                print(f"\nDate {current_date}")
                print(f"URI Levels:\n {date_df}\n")

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)


            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)

            ##############################

            fig = px.bar(
                df_plot,
                x="n_levels",
                y="count",
                color="count",
                animation_frame="date",
                title="URIs Number of Levels",
                text="count"           # show count on bars
            )

            fig.show()


        # 4
        case 'Active CoAP Machines':

            # IP INFO DATAFRAME
            ip_info_dataset = {'phase': 'O3_IpInfo', 'folder': 'csv', 'dataset': data_paths['dataset']}
            ip_info_dataset_str = files_handling.path_dict_to_str(ip_info_dataset) + '.csv'

            # considering only necessary columns
            ip_info_df = pd.read_csv(ip_info_dataset_str, usecols=['saddr'])

            #######################################

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # IN/ACTIVE DICTIONARY
                # instantiating a Counter Dictionary
                in_active_dict = {'active': date_df.shape[0], 'inactive': ip_info_df.shape[0] - date_df.shape[0]}

                ##############################

                # DICTIONARY TO DATAFRAME
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(in_active_dict.items(), columns=['in/active', 'count'])
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print('@'*50)
                print(f"\nDate {current_date}")
                print(f"In/Active Servers:\n {date_df}\n")

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)


            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)

            ##############################

            fig = px.bar(
                df_plot,
                x="in/active",
                y="count",
                color="count",
                animation_frame="date",
                title="URIs Number of Levels",
                text="count"           # show count on bars
            )

            fig.show()

        
        # 5
        case '/.well-known/core Visibility':

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                date_df.dropna(ignore_index=True, inplace=True, subset='data')

                ##############################

                # /.well-known/core RESOURCE DICTIONARY
                # instantiating a Counter Dictionary
                resources_dict = {'</.well-known/core>': 0, 'n_coap_servers': 0}
                
                # iterating over date_df rows
                for index, payload in date_df.iterrows():

                    # updating total number of coap servers
                    resources_dict['n_coap_servers'] += 1

                    uris = payload_handling.uri_list_of(payload['data'])

                    if '</.well-known/core>' in uris:
                        resources_dict['</.well-known/core>'] += 1

                ##############################

                # DICTIONARY TO DATAFRAME
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(resources_dict.items(), columns=['item', 'count'])
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print(date_df)

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)

            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)

            ##############################

            fig = px.bar(
                df_plot,
                x="item",
                y="count",
                color="item",
                animation_frame="date",
                title="</.well-known/core> visibility over time",
                text="count"           # show count on bars
            )

            fig.show()

        
        # 6
        case 'Resource Metadata':

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                date_df.dropna(ignore_index=True, inplace=True, subset='data')

                ##############################

                # RESOURCE METADATA DICTIONARY
                metadata_dict = Counter()
                
                # iterating over date_df rows
                for index, payload in date_df.iterrows():

                    resources_list = payload_handling.resource_list_of(payload['data'])

                    for payload in resources_list:
                        metadata_dict.update(payload_handling.resource_metadata_names_of(payload))

                ##############################

                # DICTIONARY TO DATAFRAME
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(metadata_dict.items(), columns=['metadata', 'count'])
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print(date_df)

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)

            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)

            ##############################

            fig = px.bar(
                df_plot,
                x="metadata",
                y="count",
                color="metadata",
                animation_frame="date",
                title="Resources' Metadata over time",
                text="count"           # show count on bars
            )

            fig.show()

        
        # 7
        case 'Content Type Metadata':

            frames = []

            for date in data_paths['date']:

                # GETTING A DATE DATASET
                date_path = {'phase': data_paths['phase'],
                             'folder': data_paths['folder'],
                             'dataset': data_paths['dataset'],
                             'date': date}
                date_df = parse_csv(date_path, mode)

                ##############################

                # DATA CLEANING
                # deleting rows with data field equal to nan
                date_df.dropna(ignore_index=True, inplace=True, subset='data')

                ##############################

                # RESOURCE CONTENT TYPE METADATA DICTIONARY
                metadata_ct_dict = Counter()
                
                # iterating over date_df rows
                for index, payload in date_df.iterrows():

                    resources_list = payload_handling.resource_list_of(payload['data'])

                    for resource in resources_list:

                        ct_value = payload_handling.get_metadata_value_of(resource, 'ct')

                        if ct_value is not None:
                            metadata_ct_dict.update(ct_value)

                ##############################

                # DICTIONARY TO DATAFRAME
                # Converting the dictionary into DataFrame
                date_df = pd.DataFrame(metadata_ct_dict.items(), columns=['content_type', 'count'])
                # take only date and ignore '.csv' part of the string
                current_date = date.split('.')[0]
                # adding column date
                date_df['date'] = datetime.datetime.strptime(current_date, '%Y-%m-%d')

                ##############################

                # STATS
                print(date_df)

                ##############################

                # APPENDING DATE DATAFRAMES TO LIST
                frames.append(date_df)

            # ALL DATES DATAFRAMES CONCATENATION
            df_plot = pd.concat(frames, ignore_index=True)

            ##############################

            fig = px.bar(
                df_plot,
                x="content_type",
                y="count",
                color="content_type",
                animation_frame="date",
                title="<Content Type> Metadata over time",
                text="count"           # show count on bars
            )

            fig.show()

    return