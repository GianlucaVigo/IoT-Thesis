from utils import files_handling

import os
import pandas as pd
        

def merging_menu():

    '''1) CoapInfo datasets selection'''
    # Levels:
    #   0. dataset (ex. 01_output.csv)
    #   1. experiment (ex. exp_0)
    #   2. date (ex. 2025-09-02)

    levels = ['dataset', 'experiment', 'date']
    path = {'phase': 'CoapInfo', 'folder': 'csv'}

    for level in levels:
        choice = files_handling.level_selection(level, path)

        if choice == None:
            return
        else: 
            path.update({level: choice})
    

    if files_handling.all_partitions_done(path):

        merged_file_path = {'phase': 'Merging',
                            'folder': 'csv',
                            'dataset': path['dataset'],
                            'experiment': path['experiment'],
                            }

        merged_file_path_str = files_handling.path_dict_to_str(merged_file_path)

        # if not already present the directory path...
        if not os.path.exists(merged_file_path_str):
            # create the Merged CoAP output directory
            os.makedirs(merged_file_path_str) 

        # Date
        merged_file_path.update({'date': f"{path['date']}.csv"})
        merged_file_path_str = files_handling.path_dict_to_str(merged_file_path)

        # Files to concatenate
        filenames = os.listdir(files_handling.path_dict_to_str(path))
        filenames.sort()

        # List containing dataframes to combine
        dataframes = []

        for file in filenames:
            # partition path
            path.update({'partition': file})
            partition_path_str = files_handling.path_dict_to_str(path)

            # CSV -> Dataframe
            data_chunk = pd.read_csv(partition_path_str)

            # Append dataframe to list
            dataframes.append(data_chunk)

        # Merge/Concatenate dataframes in the list
        combined_dataframes = pd.concat(dataframes, ignore_index=True)

        # Dataframes -> Merged CSV
        combined_dataframes.to_csv(merged_file_path_str, index=False)

    else:
        print("\n\t[ERROR] Some partitions are missing!\n\t\tPlease perform CoAP testing for all the experiment partitions generated")

    

    return None