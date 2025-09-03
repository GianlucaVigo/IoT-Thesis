import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import plotly.express as px

from utils import payload_handling

def instant_analysis(data_df, mode):

    # Plot Figure Size
    plt.figure(figsize=(8, 5))

    stat_rows = []

    match mode:

        # 0
        case 'Payload Size':
            
            # Type of plot
            sns.countplot(data=data_df, x='Payload Size (bytes)', color="lightgreen")
            
            # Plot Labels
            plt.xlabel('Payload Size (Bytes)')
            plt.ylabel("# CoAP Resources")

            # Plot Title
            plt.title("[PAYLOAD] Size Distribution")

            # Additional stats
            stat_rows.append(f"\tPayload size average value: {data_df['Payload Size (bytes)'].mean()}")
            stat_rows.append(f"\tPayload size median value: {data_df['Payload Size (bytes)'].median()}")


        # 1
        case 'Most Common':

            '''DATA PROCESSING'''
            
            uris_dict = Counter()

            for i, raw_payload in enumerate(data_df['Payload']):

                if data_df.iloc[i, 8] == False: # checking isCoAP field -> pass the check if True
                    continue

                if data_df.iloc[i, 9] != '2.05 Content': # analyse only 2.05 code responses
                    continue

                uris_list = payload_handling.uri_list_of(raw_payload)
                uris_dict.update(uris_list)

            # getting the most common 30 URIs
            # most_common() method returns a list
            top_uris = uris_dict.most_common(30)
            
            # dictionary -> pd DataFrame
            top_uris_df = pd.DataFrame(top_uris, columns=['uri', 'count'])

            # Type of plot
            sns.barplot(data=top_uris_df, y="uri", x="count", color="lightgreen")

            # Plot Labels
            plt.xlabel('Count')
            plt.ylabel("URI")

            # Plot Title
            plt.title("[PAYLOAD] TOP 30 Most Common URI")

            stat_rows.append(f"TOP 30 Most Common URI:\n {top_uris_df}")

        
        # 2
        case 'Resources Number':

            '''DATA PROCESSING'''
            n_resources_dict = Counter()
            
            for i, raw_payload in enumerate(data_df['Payload']):

                if data_df.iloc[i, 8] == False: # check wheter the machine is not exposing CoAP
                    continue

                if data_df.iloc[i, 9] != '2.05 Content': # analyse only 2.05 code responses
                    continue

                n_resources_list = [len(payload_handling.resource_list_of(raw_payload))]
                n_resources_dict.update(n_resources_list)
            
            # dictionary -> pd DataFrame
            n_resources_df = pd.DataFrame(n_resources_dict.items(), columns=['n_resources', 'count'])

            # Type of plot
            sns.barplot(data=n_resources_df, x='n_resources', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Number of Resources')
            plt.ylabel("# CoAP Machines")

            # Plot Title
            plt.title("[PAYLOAD] Number of Resources per CoAP Machine")

            stat_rows.append(f"Number of Resources per CoAP Machine:\n {n_resources_df}\n")
            stat_rows.append(f"Total Number of resources:\n {(n_resources_df['n_resources'] * n_resources_df['count']).sum()}\n")


        # 3
        case 'Resource URI Depth':

            '''DATA PROCESSING'''
            n_levels_dict = Counter()

            for i, raw_payload in enumerate(data_df['Payload']):

                if data_df.iloc[i, 8] == False: # check wheter the machine is not exposing CoAP
                    continue

                if data_df.iloc[i, 9] != '2.05 Content': # analyse only 2.05 code responses
                    continue

                uris_list = payload_handling.uri_list_of(raw_payload)

                n_levels_list = []
                for uri in uris_list:
                    n_levels_list.append(payload_handling.n_levels_of(uri))

                n_levels_dict.update(n_levels_list)

            # dictionary -> pd DataFrame
            n_levels_df = pd.DataFrame(n_levels_dict.items(), columns=['n_levels', 'count'])

            # Type of plot
            sns.barplot(data=n_levels_df, x='n_levels', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Number of Levels')
            plt.ylabel("# CoAP Resources")

            # Plot Title
            plt.title("[PAYLOAD] Number of Levels per CoAP Resource")

            stat_rows.append(f"Resource URI depth:\n {n_levels_df}")


        # 4
        case 'Active CoAP Machines':

             # Type of plot
            sns.countplot(data=data_df, x='isCoAP', color="lightgreen")

            # Plot Labels
            plt.xlabel('Active/Inactive CoAP Machine')
            plt.xticks([0, 1], ['False', 'True'])

            plt.ylabel("# CoAP Machines")

            # Plot Title
            plt.title('[PAYLOAD] Number of active/inactive CoAP machines')

            stat_rows.append(f"Active/Inactive CoAP machines:\n {data_df['isCoAP'].value_counts()}")

        
        # 5
        case 'Response Code':

             # Type of plot
            sns.countplot(data=data_df, x='Code', color="lightgreen")

            # Plot Labels
            plt.xlabel('Response Code')
            plt.ylabel("# Responses")

            # Plot Title
            plt.title('[PAYLOAD] Valid CoAP Responses')

            stat_rows.append(f"Response Codes:\n {data_df['Code'].value_counts()}")

        
        # 6
        case 'Resource Metadata':

            '''DATA PROCESSING'''
            metadata_dict = Counter()

            for i, raw_payload in enumerate(data_df['Payload']):

                if data_df.iloc[i, 8] == False: # check wheter the machine is not exposing CoAP
                    continue

                if data_df.iloc[i, 9] != '2.05 Content': # analyse only 2.05 code responses
                        continue

                resources_list = payload_handling.resource_list_of(raw_payload)

                for resource_payload in resources_list:
                    metadata_dict.update(payload_handling.resource_metadata_names_of(resource_payload))

            # dictionary -> pd DataFrame
            metadata_df = pd.DataFrame(metadata_dict.items(), columns=['metadata', 'count'])

            # Type of plot
            sns.barplot(data=metadata_df, x='metadata', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Resource Metadata')
            plt.ylabel("# Metadata Occurrences")

            # Plot Title
            plt.title("[PAYLOAD] Metadata Distribution over CoAP Resources")

            stat_rows.append(f"Metadata Distribution over CoAP Resources:\n {metadata_df}")


    '''PLOTTING'''
    plt.tight_layout()
    plt.show()

    '''STATS'''
    print("Additional statistics:\n")
    if len(stat_rows) > 0:
        for stat in stat_rows:
            print(stat)
    else:
        print("\tNo stats available")

    return



def time_analysis(data_df, mode):

    match mode:

        # 0
        case 'Payload Size':

            plt.figure(figsize=(12,6))
            sns.boxplot(data=data_df, x="Date", y="Payload Size (bytes)")
            plt.xticks(rotation=45)
            plt.title("Payload Size Distribution per Date")
            plt.show()

            plt.figure(figsize=(12,6))
            sns.stripplot(data=data_df, x="Date", y="Payload Size (bytes)", hue="country", jitter=True, alpha=0.6)
            plt.xticks(rotation=45)
            plt.title("Payload Size Distribution per Date")
            plt.show()

            fig = px.scatter(
                data_df,
                x="Date",
                y="Payload Size (bytes)",
                color="country",
                title="Payload Size Distribution per Date",
                opacity=0.7
            )

            fig.show()


        # 1
        case 'Most Common':
            
            # empty list to collect results
            all_resources = []
            
            for date in sorted(data_df['Date'].unique()):
                date_df = data_df[data_df['Date'] == date]
                uris_dict = Counter()

                for i, raw_payload in enumerate(date_df['Payload']):

                    if date_df.iloc[i, 9] == False: # check wheter the machine is not exposing CoAP
                       continue

                    if date_df.iloc[i, 10] != '2.05 Content': # analyse only 2.05 code responses
                        continue
                     
                    uris_list = payload_handling.uri_list_of(raw_payload)
                    uris_dict.update(uris_list)

                # sorting the dictionary by value
                top_uris = resources_dict.most_common(30)

                # dictionary -> pd DataFrame
                top_uris_df = pd.DataFrame(top_uris, columns=['uri', 'count'])
                top_uris_df['Date'] = date # add the date
                all_resources.append(top_uris_df)

            # concatenate all dates
            all_dates_df = pd.concat(all_resources, ignore_index=True)

            # sort by date and count
            all_dates_df = (all_dates_df.sort_values(['Date', 'count'], ascending=[True, False]).groupby('Date', group_keys=False))

            fig = px.bar(
                all_dates_df,
                x="count",
                y="uri",
                color="uri",          # optional: color by URI
                animation_frame="Date",
                orientation='h',       # horizontal bars
                title="Top 30 URIs per Date",
                text="count"           # show count on bars
            )

            fig.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)

            fig.show()

        
        # 2
        case 'Resources Number':

            # empty list to collect results
            all_resources = []
            
            for date in sorted(data_df['Date'].unique()):
                date_df = data_df[data_df['Date'] == date]
                n_resources_dict = Counter()

                for i, raw_payload in enumerate(date_df['Payload']):

                    if date_df.iloc[i, 9] == False: # check wheter the machine is not exposing CoAP
                        continue

                    if date_df.iloc[i, 10] != '2.05 Content': # analyse only 2.05 code responses
                        continue

                    n_resources_list = [len(payload_handling.uri_list_of(raw_payload))]
                    n_resources_dict.update(n_resources_list)

                # dictionary -> pd DataFrame
                n_resources_df = pd.DataFrame(n_resources_dict.items(), columns=['res_num', 'count'])
                n_resources_df['Date'] = date # add the date
                all_resources.append(n_resources_df)

            # concatenate all dates
            all_dates_df = pd.concat(all_resources, ignore_index=True)

            fig = px.bar(
                all_dates_df,
                x="res_num",
                y="count",
                color="count",
                animation_frame="Date",
                #orientation='h',       # horizontal bars
                title="Resource Number per CoAP Machine",
                text="count"           # show count on bars
            )

            fig.show()
            

        # 3
        case 'Resource URI Depth':

            # empty list to collect results
            all_resources = []
            
            for date in sorted(data_df['Date'].unique()):
                date_df = data_df[data_df['Date'] == date]
                n_levels_dict = Counter()

                for i, raw_payload in enumerate(date_df['Payload']):

                    if date_df.iloc[i, 9] == False: # check wheter the machine is not exposing CoAP
                        continue

                    if date_df.iloc[i, 10] != '2.05 Content': # analyse only 2.05 code responses
                        continue
                    
                    uris_list = payload_handling.uri_list_of(raw_payload)

                    n_levels_list = []
                    for uri in uris_list:
                        n_levels_list.append(payload_handling.n_levels_of(uri))

                    n_levels_dict.update(n_levels_list)

                # dictionary -> pd DataFrame
                n_levels_df = pd.DataFrame(n_levels_dict.items(), columns=['n_levels', 'count'])
                n_levels_df['Date'] = date # add the date
                all_resources.append(n_levels_df)

            # concatenate all dates
            all_dates_df = pd.concat(all_resources, ignore_index=True)

            fig = px.bar(
                all_dates_df,
                x="n_levels",
                y="count",
                color="count",
                animation_frame="Date",
                title="Resource URI Depth",
                text="count"           # show count on bars
            )

            fig.show()


        # 4
        case 'Active CoAP Machines':

            df_grouped = (
                data_df
                .groupby(["Date", "isCoAP", "country"])
                .size()
                .reset_index(name="count")
            )

            print(df_grouped)

        
        # 5
        case 'Response Code':

            # empty list to collect results
            all_resources = []
            
            for date in sorted(data_df['Date'].unique()):
                date_df = data_df[data_df['Date'] == date]

                resources_dict = Counter(date_df['Code'])

                # dictionary -> pd DataFrame
                resources_df = pd.DataFrame(resources_dict.items(), columns=['response_code', 'count'])
                resources_df['Date'] = date # add the date
                all_resources.append(resources_df)

            # concatenate all dates
            resource_code_df = pd.concat(all_resources, ignore_index=True)

            fig = px.bar(
                resource_code_df,
                x="response_code",
                y="count",
                color="response_code",
                animation_frame="Date",
                title="Active CoAP Machine per Dates",
                text="count"           # show count on bars
            )

            fig.show()

        
        # 6
        case 'Resource Metadata':

            '''DATA PROCESSING'''
            metadata_dict = Counter()

            for raw_payload in data_df['Payload']:
                resources_list = payload_handling.resource_list_of(raw_payload)

                for single_resource_payload in resources_list:
                    metadata_dict.update(payload_handling.resource_metadata_names_of(single_resource_payload))

            # dictionary -> pd DataFrame
            metadata_df = pd.DataFrame(metadata_dict.items(), columns=['metadata', 'count'])

            # Type of plot
            sns.barplot(data=metadata_df, x='metadata', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Resource Metadata')
            plt.ylabel("# Metadata Occurrences")

            # Plot Title
            plt.title("[PAYLOAD] Metadata Distribution over CoAP Resources")


    #sns.kdeplot(data=data_df, x='Payload Size (bytes)', fill=True, color="lightgreen")
    #sns.boxplot(data=data_df, x='Payload Size (bytes)', color="lightgreen")

    
    return