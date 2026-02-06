from O2_Analysis.options import ip
from O2_Analysis.options import payload
from O2_Analysis.options import get_resource
from O2_Analysis.options import zmap

import os

############################################

def analysis_sel():

    # Level 2
    ###########
    # from ip_info datasets -> DONE!
    ip_tree = {
        'Country': [0],     # all 
        'Continent': [1],   # all
        'AS Name': [2],     # all
        'Stability': [3]    # all
    }
    ###########
    # from cleaned datasets -> DONE!
    payload_tree = {
        'Payload Size': [0],                        # all
        'Top 30 Most Common URI': [1],              # all
        'Number of Resources / CoAP Server': [2],   # all
        'URI Depth Levels': [3],                    # all
        'Active CoAP Machines': [4],                # all
        '/.well-known/core Visibility': [5],        # all
        'Resources\' Metadata': [6],                # all
        'Content Type Metadata': [7]                # all
    }

    #----------

    # Level 1
    ###########
    # from csv datasets -> DONE!
    zmap_tree = {
        'ZMap Output Stats': [0]    # master
    }
    ###########
    # -> DONE!
    disc_tree = {
        'IP-based': [ip_tree, 0],
        'Payload-based': [payload_tree, 1]
    }
    ###########
    obs_tree = {
        
    }
    ###########
    get_tree = {
        'Data Format': [0],             # master
        'Payload Size': [1],            # master
        'Response Code': [2],           # master
        'Options': [3],                 # master
        'Server Specifications': [4],   # master
        'OBS Resources': [5],           # master
        'Home Path Info': [6]           # master
    }

    #----------

    # Level 0
    ###########
    analysis_tree = {
        'ZMap-based': [zmap_tree, 0],       # DONE!
        'Discovery-based': [disc_tree, 1],  # DONE!
        'Observe-based': [obs_tree, 2],
        'GetResource-based': [get_tree, 3]
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

############################################

def dataset_sel(analysis):

    # --------- IP info datasets ---------
    path = 'O1_DataCollection/data/'

    # --------- Scope-oriented datasets ---------
    match analysis[0]:

        case 0:
            # zmap based
            path += 'discovery/csv/'
        case 1:
            # discovery based
            path += 'discovery/'
            
            match analysis[1]:
                
                case 0:
                    path += 'ip_info/'
                        
                case 1:
                    path += 'cleaned/'
                    
        case 2:
            # observe based
            path += 'observe/'
        case 3:
            # get based
            path += 'get/'
            
    
    # Dataset files extraction
    datasets_path = []
    
    datasets_filenames = os.listdir(path)
    datasets_filenames.sort()
    
    for dataset in datasets_filenames:
        
        if dataset.endswith('.csv'):
        
            datasets_path.append(path + dataset)
            
            
        else:
            
            files = os.listdir(f'{path}{dataset}/')
            files.sort()
            
            for data in files:
                
                datasets_path.append(f"{path}{dataset}/{data}")
    
    return datasets_path
    


def perform_analysis(analysis, data_paths):

    match analysis[0]:
        
        # zmap-based
        case 0:
            zmap.analysis(data_paths)

        # discovery-based
        case 1:
                    
            match analysis[1]:

                # IP-based
                case 0:

                    match analysis[2]:

                        # Country
                        case 0:
                            ip.analysis(data_paths, "country")
                        # Continent
                        case 1:
                            ip.analysis(data_paths, "continent")
                        # AS Name
                        case 2:
                            ip.analysis(data_paths, "as_name")
                        # Stability
                        case 3:
                            ip.stability_analysis(data_paths)

                # Payload-based
                case 1:
                            
                    match analysis[2]:

                        # Size Statistics
                        case 0:
                            payload.analysis(data_paths, "Payload Size")
                        # Top 30 Most Common URI
                        case 1:
                            payload.analysis(data_paths, "Most Common")
                        # Number of Resources / CoAP Server
                        case 2:
                            payload.analysis(data_paths, "Resources Number")
                        # URI Depth Levels
                        case 3:
                            payload.analysis(data_paths, "Resource URI Depth")
                        # Active/Inactive CoAP Machines
                        case 4:
                            payload.analysis(data_paths, "Active CoAP Machines")
                        # Response Codes
                        case 5:
                            payload.analysis(data_paths, "/.well-known/core Visibility")
                        # Resources' Metadata
                        case 6:
                            payload.analysis(data_paths, "Resource Metadata")
                        # Content Type Metadata
                        case 7:
                            payload.analysis(data_paths, "Content Type Metadata")
                        # ZMap Results
                        case 8:
                            payload.analysis(data_paths, "ZMap Results")

        # observe-based
        case 2:

            print("Observe-kind of analysis")


        # get_resource-based
        case 3:
                    
            match analysis[1]:

                # Data format
                case 0:
                    get_resource.analysis(data_paths, "Data Format")
                # Payload Size
                case 1:
                    get_resource.analysis(data_paths, "Payload Size")
                # Response Code
                case 2:
                    get_resource.analysis(data_paths, "Response Code")
                # Options
                case 3:
                    get_resource.analysis(data_paths, "Options")
                # Server Specs
                case 4:
                    get_resource.analysis(data_paths, "Server Specifications")
                # Real/Fake Obs Resources
                case 5:
                    get_resource.analysis(data_paths, "OBS Resources")
                # Home Path Info
                case 6:
                    get_resource.analysis(data_paths, "Home Path Info")


    print('-'*100)
      

def analysis_menu():

    print('-' * 50, '[ANALYSIS]', '-' * 50)
    print("Extract knowledge/insights out of collected data\n")

    while(True):

        # 1) analysis selection
        chosen_analysis = analysis_sel()

        if chosen_analysis == None:
            return

        # 2) dataset/s selection
        data_paths = dataset_sel(chosen_analysis)

        if data_paths is None:
            return
    
        # 3) perform analysis
        perform_analysis(chosen_analysis, data_paths)