import pandas as pd
from collections import Counter
import plotly.express as px

from utils import payload_handling

CHUNK_SIZE = 10000


def analysis(data_paths, mode):

    match mode:

        # 0
        case 'Payload Size':

            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                sizes = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'data_length', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having code equal to 2.05 Content
                    chunk = chunk[chunk['code'] == '2.05 Content']
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    sizes += Counter(chunk['data_length'])

                to_plot.extend(
                    {'date': current_date, 'size': size, 'count': count}
                    for size, count in sizes.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # Converting the dictionary into DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'size', 'count'])
            df_plot = df_plot.groupby(['date', 'size'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'size'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.histogram(
                df_plot,
                nbins=20,
                x='size',
                y='count',
                color="date",
                barmode='group',
                title="Payload Size Distribution over Time"
            )
            fig.show()


        # 1
        case 'Most Common':
            
            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                uri_counter = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having code equal to 2.05 Content
                    chunk = chunk[chunk['code'] == '2.05 Content']
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    uri_lists = chunk['data'].apply(payload_handling.uri_list_of)

                    for uri_list in uri_lists:
                        uri_counter += Counter(uri_list)

                to_plot.extend(
                    {'date': current_date, 'uri': uri, 'count': count}
                    for uri, count in uri_counter.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'uri', 'count'])
            df_plot = df_plot.groupby(['date', 'uri'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'count'], ascending=[True, False]).reset_index(drop=True)

            df_plot = (
                df_plot
                .groupby('date', group_keys=False)
                .head(30)
            )

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='count',
                y='uri',
                animation_frame='date',
                title="30 Most Common URIs over Time"
            )
            
            fig.update_layout(yaxis = {'categoryorder': 'total ascending'})
            fig.show()

        
        # 2
        case 'Resources Number':

            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                n_resources = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having code equal to 2.05 Content
                    chunk = chunk[chunk['code'] == '2.05 Content']
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    n_resources += Counter(chunk['data'].apply(payload_handling.resource_list_of).apply(len))


                to_plot.extend(
                    {'date': current_date, 'n_res': n_res, 'count': count}
                    for n_res, count in n_resources.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'n_res', 'count'])
            df_plot = df_plot.groupby(['date', 'n_res'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'count'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='n_res',
                y='count',
                color="date",
                barmode='group',
                title="Resource Number Distribution over Time"
            )
            fig.show()


        # 3
        case 'Resource URI Depth':

            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                n_levels = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having code equal to 2.05 Content
                    chunk = chunk[chunk['code'] == '2.05 Content']
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    uri_lists = chunk['data'].apply(payload_handling.uri_list_of)

                    for uri_list in uri_lists:
                        for uri in uri_list:
                            n_levels += Counter([payload_handling.n_levels_of(uri)])

                to_plot.extend(
                    {'date': current_date, 'n_levels': n_levels, 'count': count}
                    for n_levels, count in n_levels.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'n_levels', 'count'])
            df_plot = df_plot.groupby(['date', 'n_levels'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'count'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='n_levels',
                y='count',
                color="date",
                barmode='group',
                title="Resource URI Depth Distribution over Time"
            )
            fig.show()


        # 4
        case 'Active CoAP Machines':

            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                active_servers = 0
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['success']): 
                    
                    active_servers += chunk[chunk['success'] == 1].shape[0]

                to_plot.extend(
                    [{'date': current_date, 'active_servers': active_servers}]
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'active_servers'])
            df_plot = df_plot.groupby(['date'], as_index=False)['active_servers'].sum()
            df_plot = df_plot.sort_values(['date', 'active_servers'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='date',
                y='active_servers',
                title="Active CoAP servers over Time"
            )
            fig.show()

        
        # 5
        case '/.well-known/core Visibility':

            to_plot = []
            
            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                coap_servers = 0
                wellknown_explicit = 0
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code', 'success']): 

                    # DATA CLEANING
                    # keeping only entries having code equal to 2.05 Content
                    chunk = chunk[chunk['code'] == '2.05 Content']
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    coap_servers += chunk[chunk['success'] == 1].shape[0]

                    uri_lists = chunk['data'].apply(payload_handling.uri_list_of)

                    for uri_list in uri_lists:
                        if '</.well-known/core>' in uri_list:
                            wellknown_explicit += 1

                to_plot.extend(
                    [{'date': current_date, 'coap_servers': coap_servers, 'wellknown_explicit': wellknown_explicit}]
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'coap_servers', 'wellknown_explicit'])
            df_plot = df_plot.groupby(['date'], as_index=False)[['coap_servers', 'wellknown_explicit']].sum()
            df_plot = df_plot.sort_values(['date', 'coap_servers', 'wellknown_explicit'], ascending=[True, False, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='date',
                y=['coap_servers', 'wellknown_explicit'],
                barmode='group',
                orientation='v',
                title="/.well-known/core Visibility Distribution over Time"
            )
            fig.show()
        
        # 6
        case 'Resource Metadata':

            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                metadatas = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having code equal to 2.05 Content
                    chunk = chunk[chunk['code'] == '2.05 Content']
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    resources_lists = chunk['data'].apply(payload_handling.resource_list_of)

                    for resources_list in resources_lists:
                        for resource in resources_list:
                            metadatas += Counter(payload_handling.resource_metadata_names_of(resource))


                to_plot.extend(
                    {'date': current_date, 'metadata': metadata, 'count': count}
                    for metadata, count in metadatas.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'metadata', 'count'])
            df_plot = df_plot.groupby(['date', 'metadata'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'count'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='metadata',
                y='count',
                color="date",
                barmode='group',
                title="Metadata Distribution over Time"
            )
            fig.show()


        # 7
        case 'Content Type Metadata':

            to_plot = []

            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                ct_values = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['data', 'code']): 

                    # DATA CLEANING
                    # keeping only entries having code equal to 2.05 Content
                    chunk = chunk[chunk['code'] == '2.05 Content']
                    # deleting rows with data field equal to nan
                    chunk.dropna(ignore_index=True, inplace=True, subset=['data'])

                    ##############################

                    resources_lists = chunk['data'].apply(payload_handling.resource_list_of)

                    for resources_list in resources_lists:
                        for resource in resources_list:

                            ct_value = payload_handling.get_metadata_value_of(resource, 'ct')

                            if ct_value is not None:
                                ct_values.update(ct_value)


                to_plot.extend(
                    {'date': current_date, 'ct_value': ct_value, 'count': count}
                    for ct_value, count in ct_values.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'ct_value', 'count'])
            df_plot = df_plot.groupby(['date', 'ct_value'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'count'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='ct_value',
                y='count',
                color="date",
                barmode='group',
                title="Metadata Distribution over Time"
            )
            fig.show()

        # 8
        case 'ZMap Results':

            to_plot = []
            
            for path in data_paths:

                # take only date and ignore '.csv' part of the string
                current_date = path.split('/')[4][:-4]

                zmap_results = Counter()
                
                for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, usecols=['classification', 'icmp_unreach_str', 'code']): 

                    for _, row in chunk.iterrows():

                        if row['classification'] == 'udp':
                            key = ('udp', row['code'])
                        elif row['classification'] == 'icmp':
                            key = ('icmp', row['icmp_unreach_str'])
                        else:
                            continue
                                
                        zmap_results.update([key])

                to_plot.extend(
                    {'date': current_date, 'first_level': first_level, 'second_level': second_level, 'count': count}
                    for (first_level, second_level), count in zmap_results.items()
                )

            ##############################

            # DICTIONARY TO DATAFRAME
            # list -> pd DataFrame
            df_plot = pd.DataFrame(to_plot, columns=['date', 'first_level', 'second_level', 'count'])
            df_plot = df_plot.groupby(['date', 'first_level', 'second_level'], as_index=False)['count'].sum()
            df_plot = df_plot.sort_values(['date', 'count'], ascending=[True, False]).reset_index(drop=True)

            ##############################

            # PLOTTING
            fig = px.bar(
                df_plot,
                x='second_level',             # or both combined if you like
                y='count',
                color='date',
                facet_col='first_level',      # this separates UDP vs ICMP visually
                barmode='group',
                title="ZMap Results Distribution over Time"
            )
            fig.show()

            df_plot['category'] = df_plot['first_level'] + ' / ' + df_plot['second_level'].astype(str)
            
            fig = px.bar(
                df_plot, 
                x='category', 
                y='count', 
                color='date', 
                barmode='group'
            )
            fig.show()


    return