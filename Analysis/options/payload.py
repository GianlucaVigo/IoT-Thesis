import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

from utils import payload_handling
from utils import files_handling

def instant_analysis(data_df, mode):

    # Plot Figure Size
    plt.figure(figsize=(8, 5))

    stat_rows = []

    match mode:

        # 4
        case 'Payload Size':
            
            # Type of plot
            sns.countplot(data=data_df, x='Payload Size (bytes)', color="lightgreen")
            #sns.histplot(data=data_df, x='Payload Size (bytes)', bins=40, kde=True, color="lightgreen")
            
            # Plot Labels
            plt.xlabel('Payload Size (Bytes)')
            plt.ylabel("# CoAP Resources")

            # Plot Title
            plt.title("[PAYLOAD] Size Distribution")

            # Additional stats
            stat_rows.append(f"\tPayload size average value: {data_df['Payload Size (bytes)'].mean()}")
            stat_rows.append(f"\tPayload size median value: {data_df['Payload Size (bytes)'].median()}")


        # 5
        case 'Most Common':

            '''DATA PROCESSING'''
            
            resources_dict = Counter()

            for raw_payload in data_df['Payload']:
                resources_list = payload_handling.URI_list_of(str(raw_payload))
                resources_dict.update(resources_list)

            # sorting the dictionary by value
            resources_dict = dict(resources_dict.most_common())
            
            # dictionary -> pd DataFrame
            resources_df = pd.DataFrame(resources_dict.items(), columns=['uri', 'count'])

            # Type of plot
            sns.barplot(data=resources_df.head(30), y="uri", x="count", color="lightgreen")

            # Plot Labels
            plt.xlabel('Count')
            plt.ylabel("URI")

            # Plot Title
            plt.title("[PAYLOAD] TOP 30 Most Common Resources' URI")

        
        # 6
        case 'Resources Number':

            '''DATA PROCESSING'''
            n_resources_dict = Counter()
            
            # iterate over the raw payloads
            for raw_payload in data_df['Payload']:
                n_resources_per_list = [len(payload_handling.URI_list_of(str(raw_payload)))]
                n_resources_dict.update(n_resources_per_list)
            
            # dictionary -> pd DataFrame
            n_resources_df = pd.DataFrame(n_resources_dict.items(), columns=['n_resources', 'count'])

            # Type of plot
            sns.barplot(data=n_resources_df, x='n_resources', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Number of Resources')
            plt.ylabel("# CoAP Machines")

            # Plot Title
            plt.title("[PAYLOAD] Number of Resources per CoAP Machine")


        # 7
        case 'Resource URI Depth':

            '''DATA PROCESSING'''
            levels_dict = Counter()

            for raw_payload in data_df['Payload']:
                resources_list = payload_handling.URI_list_of(str(raw_payload))

                n_levels_list = []
                for single_resource_payload in resources_list:
                    n_levels_list.append(payload_handling.n_levels_of(single_resource_payload))

                levels_dict.update(n_levels_list)

            # dictionary -> pd DataFrame
            levels_df = pd.DataFrame(levels_dict.items(), columns=['n_levels', 'count'])

            # Type of plot
            sns.barplot(data=levels_df, x='n_levels', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Number of Levels')
            plt.ylabel("# CoAP Resources")

            # Plot Title
            plt.title("[PAYLOAD] Number of Levels per CoAP Resource")


        # 8
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

        
        # 9
        case 'Response Code':

             # Type of plot
            sns.countplot(data=data_df, x='Code', color="lightgreen")

            # Plot Labels
            plt.xlabel('Response Code')
            plt.ylabel("# Responses")

            # Plot Title
            plt.title('[PAYLOAD] Valid CoAP Responses')

            stat_rows.append(f"Response Codes:\n {data_df['Code'].value_counts()}")

        
        # 10
        case 'Resource Metadata':

            '''DATA PROCESSING'''
            metadata_dict = Counter()

            for raw_payload in data_df['Payload']:
                resources_list = payload_handling.resource_list_of(raw_payload)

                for single_resource_payload in resources_list:
                    metadata_dict.update(payload_handling.resource_attributes(str(single_resource_payload)))

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


    print("-" * 100)
    return