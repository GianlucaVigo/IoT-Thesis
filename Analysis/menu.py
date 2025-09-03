import pandas as pd
import os

from Analysis.options import ip
from Analysis.options import payload

from utils import files_handling


def analysis_sel():

    # Level 3
    ip_tree = {'Country': [0],
                'Continent': [1],
                'AS Name': [2]
                }
    
    payload_tree = {'Size Statistics': [0],
                    'Top 30 Most Common URI': [1],
                    'Number of Resources / CoAP Server': [2],
                    'URI Depth Levels': [3],
                    'Active/Inactive CoAP Machines': [4],
                    'Response Codes': [5],
                    'Resources\' Metadata': [6]
                    }
    ###########
    #----------

    # Level 2
    instant_tree = {'IP-based': [ip_tree, 0],
                    'Payload-based': [payload_tree, 1]
                    }
    
    time_tree = {'IP-based': [ip_tree, 0],
                'Payload-based': [payload_tree, 1]
                }
    ###########
    #----------

    # Level 1
    discovery_tree = {'Instant-based': [instant_tree, 0],
                      'Time-based': [time_tree, 1]
                     }
    ###########
    observe_tree = {'Prova': [0]}
    #----------

    # Level 0
    analysis_tree = {'Discovery-based': [discovery_tree, 0],
                      'Observe-based': [observe_tree, 1]
                      }
    
    ''''''

    user_tree = analysis_tree
    user_selection = []

    while(True):

        print(f"On which kind of analysis are you interested in?\n")

        options = list(user_tree.keys())
            
        for option_id, option in enumerate(options):
            print(f"\t{option_id:2d}. {option}")


        #   User selection
        print(f"\nSelect the index: ['e' to main menu] ")
        try:
            choice = input()

            if (choice == 'e'):
                print("-" * 100)
                return # -> to MAIN MENU
                            
            choice = int(choice)

            chosen_tree_list = user_tree[options[choice]]

        except Exception as e:
            print("\tINPUT ERROR: Invalid input")    # Invalid user input
            print(f"\t\t {e}")
            print("-" * 100)
            continue

        else:

            if (choice not in range(len(options))):
                print("\tINPUT ERROR: Invalid option -> Out of range!")    # Invalid user choice
                print("-" * 100)
                continue
                    
            elif (len(chosen_tree_list) == 2): # Go to next level

                user_tree = chosen_tree_list[0]             # take next tree menù
                user_selection.append(chosen_tree_list[1])  # append number identifying user choice
                print("-" * 100)

            else: # Menù end reached

                user_selection.append(chosen_tree_list[0])
                print("-" * 100)
                break


    return user_selection


def dataset_sel(analysis):

    # discovery based
    if analysis[0] == 0:

        # instant based -> ONLY 1 dataset to consider
        if analysis[1] == 0:

            levels = ["dataset", "experiment", "date"]
            path = {'phase': 'Merging', 'folder': 'csv'}

            for level in levels:
                    choice = files_handling.level_selection(level, path)

                    if choice == None:
                        return None
                    else: 
                        path.update({level: choice})
            
            # preloaded dataframe
            data_df = pd.read_csv(files_handling.path_dict_to_str(path))

            return data_df

        # time-series based -> MULTIPLE datasets must be considered
        elif analysis[1] == 1:
            
            levels = ["dataset", "experiment"]
            path = {'phase': 'Merging', 'folder': 'csv'}

            for level in levels:
                    choice = files_handling.level_selection(level, path)

                    if choice == None:
                        return None
                    else: 
                        path.update({level: choice})

            csvs = os.listdir(files_handling.path_dict_to_str(path))
            csvs.sort()

            frames = []

            for i, csv in enumerate(csvs):
                path.update({'date': csv})
            
                if i == 0:
                    date_df = pd.read_csv(files_handling.path_dict_to_str(path), skiprows=0)
                else:
                    date_df = pd.read_csv(files_handling.path_dict_to_str(path))

                date_df['Date'] = csv.split('.')[0]

                frames.append(date_df)

            data_df = pd.concat(frames).reset_index()

            return data_df
    
    # Observe Based
    else:
        print("0bserve-based analysis")



def perform_analysis(analysis, dataset):

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
                                    ip.instant_analysis(dataset, "country")
                                # Continent
                                case 1:
                                    ip.instant_analysis(dataset, "continent")
                                # AS Name
                                case 2:
                                    ip.instant_analysis(dataset, "as_name")

                        # Payload-based
                        case 1:
                            
                            match analysis[3]:

                                # Size Statistics
                                case 0:
                                    payload.instant_analysis(dataset, "Payload Size")
                                # Top 30 Most Common URI
                                case 1:
                                    payload.instant_analysis(dataset, "Most Common")
                                # Number of Resources / CoAP Server
                                case 2:
                                    payload.instant_analysis(dataset, "Resources Number")
                                # URI Depth Levels
                                case 3:
                                    payload.instant_analysis(dataset, "Resource URI Depth")
                                # Active/Inactive CoAP Machines
                                case 4:
                                    payload.instant_analysis(dataset, "Active CoAP Machines")
                                # Response Codes
                                case 5:
                                    payload.instant_analysis(dataset, "Response Code")
                                # Resources' Metadata
                                case 6:
                                    payload.instant_analysis(dataset, "Resource Metadata") 

                # time-based
                case 1:
                    print("Time based analysis")

                    match analysis[2]:

                        # IP-based
                        case 0:

                            match analysis[3]:

                                # Country
                                case 0:
                                    ip.time_analysis(dataset, "country")
                                # Continent
                                case 1:
                                    ip.time_analysis(dataset, "continent")
                                # AS Name
                                case 2:
                                    ip.time_analysis(dataset, "as_name")

                        # Payload-based
                        case 1:
                            
                            match analysis[3]:

                                # Size Statistics
                                case 0:
                                    payload.time_analysis(dataset, "Payload Size")
                                # Top 30 Most Common URI
                                case 1:
                                    payload.time_analysis(dataset, "Most Common")
                                # Number of Resources / CoAP Server
                                case 2:
                                    payload.time_analysis(dataset, "Resources Number")
                                # URI Depth Levels
                                case 3:
                                    payload.time_analysis(dataset, "Resource URI Depth")
                                # Active/Inactive CoAP Machines
                                case 4:
                                    payload.time_analysis(dataset, "Active CoAP Machines")
                                # Response Codes
                                case 5:
                                    payload.time_analysis(dataset, "Response Code")
                                # Resources' Metadata
                                case 6:
                                    payload.time_analysis(dataset, "Resource Metadata") 

        # observe-based
        case 1:
            print('Observe')

    print('-'*100)
      

def analysis_menu():

    while(True):

        # 1) analysis selection
        chosen_analysis = analysis_sel()

        if chosen_analysis == None:
            return

        # 2) dataset/s selection
        data_df = dataset_sel(chosen_analysis)

        if data_df is None or data_df.empty:
            return
        
        # 3) perform analysis
        perform_analysis(chosen_analysis, data_df)