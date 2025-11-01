from O2_Analysis.options import ip
from O2_Analysis.options import payload
from O2_Analysis.options import get_resource

from utils import files_handling

############################################

APPROACH = 'b'

############################################

def analysis_sel():

    # Level 2
    ###########
    ip_tree = {
        'Country': [0],
        'Continent': [1],
        'AS Name': [2],
        'Stability': [3]
    }
    ###########
    payload_tree = {
        'Payload Size': [0],
        'Top 30 Most Common URI': [1],
        'Number of Resources / CoAP Server': [2],
        'URI Depth Levels': [3],
        'Active CoAP Machines': [4],
        '/.well-known/core Visibility': [5],
        'Resources\' Metadata': [6],
        'Content Type Metadata': [7],
        'ZMap Results': [8]
    }

    #----------

    # Level 1
    ###########
    disc_tree = {
        'IP-based': [ip_tree, 0],
        'Payload-based': [payload_tree, 1]
    }
    ###########
    obs_tree = {
        
    }
    ###########
    get_tree = {
        'Data Format': [0],
        'Payload Size': [1],
        'Response Code': [2],
        'Options': [3],
        'Server Specifications': [4],
        'OBS Resources': [5]
    }

    #----------

    # Level 0
    ###########
    analysis_tree = {
        'Discovery-based': [disc_tree, 0],
        #'Observe-based': [obs_tree, 1],
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

    # --------- IP info datasets ---------
    path = {'phase': f'O1{APPROACH}_DataCollection', 'folder': 'discovery/ip_info'}

    ip_info_datasets = files_handling.read_file_system(files_handling.path_dict_to_str(path))
    ip_info_datasets.sort()

    # --------- Scope-oriented datasets ---------
    match analysis[0]:

        case 0:
            # discovery based
            path.update({'folder': 'discovery/cleaned'})
        case 1:
            # observe based
            path.update({'folder': 'observe'})
        case 2:
            # get based
            path.update({'folder': 'get'})

    datasets = files_handling.read_file_system(files_handling.path_dict_to_str(path))
    datasets.sort()
    
    return [datasets, ip_info_datasets]
    

def perform_analysis(analysis, data_paths):

    match analysis[0]:

        # discovery-based
        case 0:
                    
            match analysis[1]:

                # IP-based
                case 0:

                    match analysis[2]:

                        # Country
                        case 0:
                            ip.analysis(data_paths[1], "country")
                        # Continent
                        case 1:
                            ip.analysis(data_paths[1], "continent")
                        # AS Name
                        case 2:
                            ip.analysis(data_paths[1], "as_name")
                        # Stability
                        case 3:
                            ip.stability_analysis(data_paths[1])

                # Payload-based
                case 1:
                            
                    match analysis[2]:

                        # Size Statistics
                        case 0:
                            payload.analysis(data_paths[0], "Payload Size")
                        # Top 30 Most Common URI
                        case 1:
                            payload.analysis(data_paths[0], "Most Common")
                        # Number of Resources / CoAP Server
                        case 2:
                            payload.analysis(data_paths[0], "Resources Number")
                        # URI Depth Levels
                        case 3:
                            payload.analysis(data_paths[0], "Resource URI Depth")
                        # Active/Inactive CoAP Machines
                        case 4:
                            payload.analysis(data_paths[0], "Active CoAP Machines")
                        # Response Codes
                        case 5:
                            payload.analysis(data_paths[0], "/.well-known/core Visibility")
                        # Resources' Metadata
                        case 6:
                            payload.analysis(data_paths[0], "Resource Metadata")
                        # Content Type Metadata
                        case 7:
                            payload.analysis(data_paths[0], "Content Type Metadata")
                        # ZMap Results
                        case 8:
                            payload.analysis(data_paths[0], "ZMap Results")

        # observe-based
        case 1:

            print("Observe-kind of analysis")


        # get_resource-based
        case 2:
                    
            match analysis[1]:

                # Data format
                case 0:
                    get_resource.analysis(data_paths[0], "Data Format")
                # Payload Size
                case 1:
                    get_resource.analysis(data_paths[0], "Payload Size")
                # Response Code
                case 2:
                    get_resource.analysis(data_paths[0], "Response Code")
                # Options
                case 3:
                    get_resource.analysis(data_paths[0], "Options")
                # Server Specs
                case 4:
                    get_resource.analysis(data_paths[0], "Server Specifications")
                # Real/Fake Obs Resources
                case 5:
                    get_resource.analysis(data_paths[0], "OBS Resources")


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