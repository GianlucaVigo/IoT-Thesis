import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

from utils import payload_handling
from utils import files_handling

def analysis(data_df, mode):

    # Plot horizontal bar chart
    plt.figure(figsize=(8, 5))

    match mode:
        case 'Payload Size (bytes)':
            sns.histplot(data=data_df, x=mode, bins=40, kde=True, color="lightgreen")
            plt.ylabel("# CoAP Resources")
            plt.title("[PAYLOAD] Size Distribution")
    #sns.kdeplot(data=data_df, x='Payload Size (bytes)', fill=True, color="lightgreen")
    #sns.boxplot(data=data_df, x='Payload Size (bytes)', color="lightgreen")

    '''PLOTTING'''
    plt.xlabel(mode)
    plt.tight_layout()
    plt.show()

    '''STATS'''
    match mode:
        case 'Payload Size (bytes)':
            # Average Payload Size
            print(f"\tPayload size average value: {data_df[mode].mean()}")
            # Payload Size Median Value
            print(f"\tPayload size median value: {data_df[mode].median()}")
    
    print("-" * 100)
    return


''''''
def size_stats(data):
    return
    



''''''
def most_common(data):

    print("[PAYLOAD] Most common Resource URIs")

    '''DATA CONVERSION'''
    # CSV -> pandas dataframe
    data_df = pd.read_csv(files_handling.path_dict_to_str(data))

    '''DATA FILTERING'''
    # consider only 'Payload' column + do not consider rows with NaN values
    payload_df = data_df.loc[:, 'Payload'].dropna(how='any', axis=0)

    '''DATA PROCESSING'''
    resource_list = []
    # resource lists
    for raw_payload in payload_df:
        resource_list.extend(payload_handling.resource_list_of(raw_payload))
    # dictionary -> pd DataFrame
    resources_dict = Counter(resource_list).most_common()
    resources_dict_df = pd.DataFrame(resources_dict, columns=['uri', 'count'])

    '''PLOTTING'''
    # plot resource_list
    sns.set_theme()
    # setting up the plot
    plot = sns.barplot(data=resources_dict_df, x="uri", y="count")
    plot.tick_params(axis='x', labelrotation=90)
    # show the plot
    plt.show()

    print("-" * 100)
    return



''''''
def n_resources(data):

    print("[PAYLOAD] Number of Resources for each IP address")

    '''DATA CONVERSION'''
    # CSV -> pandas dataframe
    data_df = pd.read_csv(files_handling.path_dict_to_str(data))

    '''DATA FILTERING'''
    # consider only 'Payload' column + do not consider rows with NaN values
    payload_df = data_df.loc[:, 'Payload'].dropna(how='any', axis=0)

    '''DATA PROCESSING'''
    # list containing the number of resources for each ip address
    n_resources_per_list = []
    # iterate over the raw payloads
    for raw_payload in payload_df:
        # append to the n_resources_per_list the size of resource_list_payload
        n_resources_per_list.append(len(payload_handling.resource_list_of(raw_payload)))
    # list -> pandas dataframe
    n_resources_per_list_df = pd.DataFrame(n_resources_per_list, columns=['n_resources'])

    '''PLOTTING'''
    sns.set_theme()
    # Plot all responses' payload size
    plot = sns.displot(data=n_resources_per_list_df)
    plot.set_xticklabels(rotation=45)
    # show the plot
    plt.show()

    '''STATS'''
    # Print average payload size
    print(f"\tAverage number of resources per machine: {n_resources_per_list_df.mean()}")
    
    print("-" * 100)
    return



''''''
def resources_depth(data):

    print("[PAYLOAD] Resource URI depth levels")

    '''DATA CONVERSION'''
    # CSV -> pandas dataframe
    data_df = pd.read_csv(files_handling.path_dict_to_str(data))

    '''DATA FILTERING'''
    # consider only 'Payload' column + do not consider rows with NaN values
    payload_df = data_df.loc[:, 'Payload'].dropna(how='any', axis=0)

    '''DATA PROCESSING'''
    n_levels_list = []
    for raw_payload in payload_df:
        resources_list = payload_handling.resource_list_of(raw_payload)

        for single_resource_payload in resources_list:
            n_levels_list.append(payload_handling.n_levels_of(single_resource_payload))

    # dictionary -> pd DataFrame
    levels_dict = Counter(n_levels_list)
    levels_dict_df = pd.DataFrame(levels_dict.items(), columns=['n_levels', 'count'])

    '''PLOTTING'''
    # plot resource_list
    sns.set_theme()
    # setting up the plot
    plot = sns.barplot(data=levels_dict_df, x="n_levels", y="count")
    plot.tick_params(axis='x', labelrotation=90)
    # show the plot
    plt.show()

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
