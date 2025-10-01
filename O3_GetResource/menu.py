import asyncio
import pandas as pd
import datetime

from utils import files_handling
from O3_GetResource import coap

def getresource_menu():

    # Header
    print('-' * 50, '[GET RESOURCE]', '-' * 50)
    # Description
    print("Interact with CoAP resources and retrieve their content through a simple GET request\n")


    '''1) Partition selection'''
    # Levels:
    #   0. dataset (ex. 01_output)
    #   1. experiment (ex. exp_1)
    #   2. date (ex. 2025-09-17)
    #   3. partition (ex. 3)

    levels = ['dataset', 'experiment', 'date', 'partition']
    path = {'phase': 'O2_ZMapDecoded', 'folder': 'csv'}

    for level in levels:

        if level == 'date':
            path.update({level: str(datetime.date.today())})

        else:
            choice = files_handling.level_selection(level, path)

            if choice == None:
                return
            
            else: 
                path.update({level: choice})

    # getting n_ips
    date_path = path.copy()
    date_path.pop('experiment')
    date_path.pop('partition')

    date_df = pd.read_csv(files_handling.path_dict_to_str(date_path) + '.csv')

    n_ips = date_df.shape[0]

    #########################

    # getting number of partitions
    experiments = files_handling.get_exps_info(path['dataset'])

    for exp in experiments:
        
        if exp['experiment'] == path['experiment']:
            n_partitions = exp['n_partitions']
            break

    #########################
 
    # partition size
    partitions_size = n_ips // n_partitions
    # last partition will get the remaining entries
    remainder = n_ips % n_partitions

    #########################

    # PARTITION BOUNDARIES
    # start label
    start = path['partition'] * partitions_size + 1

    # end-label
    end = start + partitions_size
    # end-label update if last partition
    if path['partition'] == n_partitions - 1: 
        end += remainder

    #########################

    # is the discovery available?
    if files_handling.is_discovery_available(path['dataset']):
    
        asyncio.run(coap.get_requests(path, start, end))

    else:
        print("\nThe Resource Discovery has not been done yet (-> executes ZMap)\n")
    