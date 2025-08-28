import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

from utils import payload_handling
from utils import files_handling

def analysis(data_df, mode):

    # Plot Figure Size
    plt.figure(figsize=(8, 5))

    stat_rows = []

    match mode:

        # 4
        case 'Payload Size':
            
            # Type of plot
            sns.histplot(data=data_df, x='Payload Size (bytes)', bins=40, kde=True, color="lightgreen")
            
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
                resources_list = payload_handling.resource_list_of(str(raw_payload))
                resources_dict.update(resources_list)

            # sorting the dictionary by value
            resources_dict = dict(resources_dict.most_common())
            
            # dictionary -> pd DataFrame
            resources_df = pd.DataFrame(resources_dict.items(), columns=['uri', 'count'])
            print(resources_df.head(30))

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
                n_resources_per_list = [len(payload_handling.resource_list_of(str(raw_payload)))]
                n_resources_dict.update(n_resources_per_list)
            
            # dictionary -> pd DataFrame
            n_resources_df = pd.DataFrame(n_resources_dict.items(), columns=['n_resources', 'count'])
            print(n_resources_df)

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
                resources_list = payload_handling.resource_list_of(str(raw_payload))

                n_levels_list = []
                for single_resource_payload in resources_list:
                    n_levels_list.append(payload_handling.n_levels_of(single_resource_payload))

                levels_dict.update(n_levels_list)

            # dictionary -> pd DataFrame
            levels_df = pd.DataFrame(levels_dict.items(), columns=['n_levels', 'count'])
            
            # printing the exact numbers
            print(levels_df)

            # Type of plot
            sns.barplot(data=levels_df, x='n_levels', y='count', color="lightgreen")

            # Plot Labels
            plt.xlabel('Number of Levels')
            plt.ylabel("# CoAP Resources")

            # Plot Title
            plt.title("[PAYLOAD] Number of Levels per CoAP Resource")




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



''''''
def n_coap_servers(data):

    print("[PAYLOAD] Number of CoAP servers")

    '''DATA CONVERSION'''
    # CSV -> pandas dataframe
    data_df = pd.read_csv(files_handling.path_dict_to_str(data))

    '''DATA FILTERING'''
    # consider only 'Payload' column + do not consider rows with NaN values
    isCoAP_df = data_df.loc[:, 'isCoAP'].dropna(how='any', axis=0)

    '''PLOTTING'''
    sns.set_theme()
    # Plot all responses' payload size
    sns.countplot(x=isCoAP_df)
    plt.xticks([0, 1], ['False', 'True'])
    plt.title('Count of isCoAP values')
    # show the plot
    plt.show()

    '''STATS'''
    # Average Payload Size
    print(f"Payload size average value: {isCoAP_df.value_counts()}")
    
    print("-" * 100)
    return



''''''
def response_code_distribution(data):

    print("[PAYLOAD] Response Code Distribution")

    '''DATA CONVERSION'''
    # CSV -> pandas dataframe
    data_df = pd.read_csv(files_handling.path_dict_to_str(data))

    '''DATA FILTERING'''
    # consider only 'Code' column + do not consider rows with NaN values
    payload_size_df = data_df.loc[:, 'Code'].dropna(how='any', axis=0)

    '''PLOTTING'''
    sns.set_theme()
    # Plot all responses' payload size
    sns.displot(data=payload_size_df)
    # show the plot
    plt.show()

    print("-" * 100)
    return



''''''
def payload_attributes_distribution(data):

    print("[PAYLOAD] Resource Metadata Distribution")

    '''DATA CONVERSION'''
    # CSV -> pandas dataframe
    data_df = pd.read_csv(files_handling.path_dict_to_str(data))

    '''DATA FILTERING'''
    # consider only 'Payload' column + do not consider rows with NaN values
    payload_df = data_df.loc[:, 'Payload'].dropna(how='any', axis=0)

    '''DATA PROCESSING'''
    attributes_dict = Counter({})

    for raw_payload in payload_df:

        raw_payload = raw_payload[2:-1]

        resources_list = raw_payload.split(',')

        for single_resource_payload in resources_list:
            res = payload_handling.resource_attributes(single_resource_payload)

            if res != None:
                attributes_dict.update(res)

    attributes_df = pd.DataFrame(attributes_dict.items(), columns=['attributes', 'count'])

    '''PLOTTING'''
    # plot resource_list
    sns.set_theme()
    # setting up the plot
    plot = sns.barplot(data=attributes_df, x="attributes", y="count")
    plot.tick_params(axis='x', labelrotation=90)
    # show the plot
    plt.show()

    print("-" * 100)
    return
