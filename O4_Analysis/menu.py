import pandas as pd
import os

from O4_Analysis.options import ip
from O4_Analysis.options import payload
from O4_Analysis.options import get_resource
from O4_Analysis.options import observe

from utils import files_handling


def analysis_sel():

    # Level 3
    ###########
    ip_tree = {
        'Country': [0],
        'Continent': [1],
        'AS Name': [2]
    }
    ###########
    payload_tree = {
        'Payload Size': [0],
        'Top 30 Most Common URI': [1],
        'Number of Resources / CoAP Server': [2],
        'URI Depth Levels': [3],
        'Active/Inactive CoAP Machines': [4],
        '/.well-known/core Visibility': [5],
        'Resources\' Metadata': [6],
        'Content Type Metadata': [7],
        'ZMap Results': [8]
    }

    #----------

    # Level 2
    ###########
    disc_tree = {
        'IP-based': [ip_tree, 0],
        'Payload-based': [payload_tree, 1]
    }
    ###########
    obs_tree = {
        'Real/Fake obs resources': [0]
    }
    ###########
    get_tree = {
        'Data Format': [0],
        'Payload Size': [1],
        'Response Code': [2],
        'Options': [3],
        'Server Specifications': [4]
    }

    #----------

    # Level 1
    ###########
    discovery_tree = {
        'Stability-based': [disc_tree, 0],
        'Evolution-based': [disc_tree, 1]
    }
    ###########
    observe_tree = {
        'Stability-based': [obs_tree, 0],
        'Evolution-based': [obs_tree, 1]
    }
    ###########
    get_tree = {
        'Stability-based': [get_tree, 0],
        'Evolution-based': [get_tree, 1]
    }

    #----------

    # Level 0
    ###########
    analysis_tree = {
        'Discovery-based': [discovery_tree, 0],
        'Observe-based': [observe_tree, 1],
        'GetResource-based': [get_tree, 2]
    }
    
    #----------

    user_tree = analysis_tree
    user_selection = []

    while(True):

        print("$" * 75)
        print(f"On which kind of analysis are you interested in?\n")

        options = list(user_tree.keys())
            
        for option_id, option in enumerate(options):
            print(f"\t{option_id:2d}. {option}")


        #   User selection
        print(f"\nSelect the index: ['e' to main menu] ")
        try:
            choice = input()

            if (choice == 'e'):
                print("\n\t[Redirection to main menu ...]\n")
                return # -> to MAIN MENU
                            
            choice = int(choice)

            chosen_tree_list = user_tree[options[choice]]

        except Exception as e:
            print("\tINPUT ERROR: Invalid input")    # Invalid user input
            print(f"\t\t {e}")
            continue

        else:

            if (choice not in range(len(options))):
                print("\tINPUT ERROR: Invalid option -> Out of range!")    # Invalid user choice
                continue
                    
            elif (len(chosen_tree_list) == 2): # Go to next level

                user_tree = chosen_tree_list[0]             # take next tree menù
                user_selection.append(chosen_tree_list[1])  # append number identifying user choice

            else: # Menù end reached

                user_selection.append(chosen_tree_list[0])
                break

    return user_selection


def dataset_sel(analysis):

    path = {}

    match analysis[0]:

        case 0:
            # discovery based
            path.update({'phase': 'O1_ZMap/scan_results', 'folder': 'cleaned'})
        case 1:
            # observe based
            path.update({'phase': 'O3_Observe', 'folder': 'csv'})
        case 2:
            # get based
            path.update({'phase': 'O2_GetResource', 'folder': 'csv'})


    # stability-based
    if analysis[1] == 0:

        cleaned_datasets = files_handling.read_file_system(files_handling.path_dict_to_str(path))
        path.update({'folder': 'ip_info'})
        ip_info_datasets = files_handling.read_file_system(files_handling.path_dict_to_str(path))

        master_dates = set()

        for file_path in cleaned_datasets:
            date = file_path.split('/')[4]
            master_dates.update([date])

        ###
        # available_dates is the UNION of all partitions dates
        # ex.
        # 0 > 2025-10-15
        #   > 2025-10-16
        # 1 > 2025-10-15
        #   > 2025-10-17
        # ...
        # 6 > 2025-10-18
        #
        # available_dates = {2025-10-15, 2025-10-16, 2025-10-17, 2025-10-18
        ##


        print("Here's the list of available dates:\n")
        for i, date in enumerate(master_dates):
            print(f"\t{i}. {date}")

        #   User selects index
        print(f"\nSelect the date index: ['e' to main menu] ")

        try:
            choice = input()

            if (choice == 'e'):
                print("\n\t[Redirection to main menu ...]\n")
                return None # -> to MAIN MENU
                
            choice_id = int(choice)

        except Exception as e:
            print("\tINPUT ERROR: Invalid input\n")    # Invalid user input
            print(f"\t\t {e}")
            return None
            
        if (choice_id not in range(len(master_dates))):
            print("\tINPUT ERROR: Invalid option -> Out of range!\n")    # Invalid user choice
            return None
        
        master_dates = list(master_dates)

        print(f"You have selected: {master_dates[choice_id]}")


        cleaned_datasets_filtered = []
        ip_info_datasets_filtered = []


        # cleaned datasets
        for path in cleaned_datasets:
            if path.split('/')[4] == master_dates[choice_id]:
                cleaned_datasets_filtered.append(path)

        for path in ip_info_datasets:
            if path.split('/')[4] == f"{master_dates[choice_id]}.csv":
                ip_info_datasets_filtered.append(path)

        print(cleaned_datasets_filtered)
        print(ip_info_datasets_filtered)

        return

        

    # evolution-based
    elif analysis[1] == 1:

        for level in levels:
            choice = files_handling.level_selection(level, path)

            if choice == None:
                return None
            else: 
                path.update({level: choice})

        # elements can be a list of files (csv) or a list of directories
        elements = os.listdir(files_handling.path_dict_to_str(path))
        elements.sort()

        # discovery -> list of csvs
        if analysis[0] == 0:

            csvs_path = path.copy()

            csvs_path.update({'date': elements})

            data_paths = csvs_path
        
        # observe & get -> list of directories
        else:
            data_paths = []
            for element in elements:

                dir_path = path.copy()

                dir_path.update({'date': element})
                dir_path.update({'partition': files_handling.extract_partitions(dir_path)})

                data_paths.append(dir_path)
    
    if data_paths is None:
            return None
    else: 
        return data_paths


def perform_analysis(analysis, data_paths):

    match analysis[0]:

        # discovery-based
        case 0:
            
            match analysis[1]:

                # instant-based
                case 0:
                    
                    match analysis[2]:

                        # IP-based
                        case 0:

                            match analysis[3]:

                                # Country
                                case 0:
                                    ip.instant_analysis(data_paths, "country")
                                # Continent
                                case 1:
                                    ip.instant_analysis(data_paths, "continent")
                                # AS Name
                                case 2:
                                    ip.instant_analysis(data_paths, "as_name")

                        # Payload-based
                        case 1:
                            
                            match analysis[3]:

                                # Size Statistics
                                case 0:
                                    payload.instant_analysis(data_paths, "Payload Size")
                                # Top 30 Most Common URI
                                case 1:
                                    payload.instant_analysis(data_paths, "Most Common")
                                # Number of Resources / CoAP Server
                                case 2:
                                    payload.instant_analysis(data_paths, "Resources Number")
                                # URI Depth Levels
                                case 3:
                                    payload.instant_analysis(data_paths, "Resource URI Depth")
                                # Active/Inactive CoAP Machines
                                case 4:
                                    payload.instant_analysis(data_paths, "Active CoAP Machines")
                                # Response Codes
                                case 5:
                                    payload.instant_analysis(data_paths, "/.well-known/core Visibility")
                                # Resources' Metadata
                                case 6:
                                    payload.instant_analysis(data_paths, "Resource Metadata")
                                # Content Type Metadata
                                case 7:
                                    payload.instant_analysis(data_paths, "Content Type Metadata")

                # time-based
                case 1:

                    match analysis[2]:

                        # IP-based
                        case 0:

                            match analysis[3]:

                                # Country
                                case 0:
                                    ip.time_analysis(data_paths, "country")
                                # Continent
                                case 1:
                                    ip.time_analysis(data_paths, "continent")
                                # AS Name
                                case 2:
                                    ip.time_analysis(data_paths, "as_name")

                        # Payload-based
                        case 1:
                            
                            match analysis[3]:

                                # Size Statistics
                                case 0:
                                    payload.time_analysis(data_paths, "Payload Size")
                                # Top 30 Most Common URI
                                case 1:
                                    payload.time_analysis(data_paths, "Most Common")
                                # Number of Resources / CoAP Server
                                case 2:
                                    payload.time_analysis(data_paths, "Resources Number")
                                # URI Depth Levels
                                case 3:
                                    payload.time_analysis(data_paths, "Resource URI Depth")
                                # Active/Inactive CoAP Machines
                                case 4:
                                    payload.time_analysis(data_paths, "Active CoAP Machines")
                                # Response Codes
                                case 5:
                                    payload.time_analysis(data_paths, "/.well-known/core Visibility")
                                # Resources' Metadata
                                case 6:
                                    payload.time_analysis(data_paths, "Resource Metadata")
                                # Content Type Metadata
                                case 7:
                                    payload.time_analysis(data_paths, "Content Type Metadata") 

        # observe-based
        case 1:
            print('Observe')

        # get_resource-based
        case 2:
            
            match analysis[1]:

                # instant-based
                case 0:
                    
                    match analysis[2]:

                        # Data format
                        case 0:
                            get_resource.instant_analysis(data_paths, "Data Format")
                        # Payload Size
                        case 1:
                            get_resource.instant_analysis(data_paths, "Payload Size")
                        # Response Code
                        case 2:
                            get_resource.instant_analysis(data_paths, "Response Code")
                        # Options
                        case 3:
                            get_resource.instant_analysis(data_paths, "Options")


                # time-based
                case 1:

                    match analysis[2]:

                        # Data format
                        case 0:
                            get_resource.time_analysis(data_paths, "Data Format")
                        # Payload Size
                        case 1:
                            get_resource.time_analysis(data_paths, "Payload Size")
                        # Response Code
                        case 2:
                            get_resource.time_analysis(data_paths, "Response Code")
                        # Options
                        case 3:
                            get_resource.time_analysis(data_paths, "Options")


    print('-'*100)
      

def analysis_menu():

    print('-' * 50, '[ANALYSIS]', '-' * 50)
    print("Extract knowledge/insights out of collected data\n")

    while(True):

        # 1) analysis selection
        chosen_analysis = analysis_sel()

        if chosen_analysis == None:
            return
        
        print(chosen_analysis)

        # 2) dataset/s selection
        data_paths = dataset_sel(chosen_analysis)

        return

        # debug
        print('ç' * 50)
        print(f"dataset paths:\n {data_paths}")

        if data_paths is None:
            return
        
        # 3) perform analysis
        perform_analysis(chosen_analysis, data_paths)