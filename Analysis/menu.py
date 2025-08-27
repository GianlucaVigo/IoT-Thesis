import pandas as pd

from Analysis.options import ip
from Analysis.options import payload

from utils import files_handling


# return the analysis to perform requested by user 
def analysis_selection():

    analysis_list = ["country", # IP analysis
                    "continent",
                    "as_name",
                    "stability", # same zmap source, different experiments over time

                    "size_stats", # Response Msg analysis
                    "most_common",
                    "#_resources",
                    "resources_depth",
                    "#_coap_servers",
                    "response_code_distribution",
                    "payload_attributes_distribution"
                    ]
    
    selected_analysis = None
        
    print(f"On which kind of analysis are you interested?\n")
    
    for option_id in range(len(analysis_list)):
        print(f"\t{option_id:2d}. {analysis_list[option_id]}")

    while(True):

        #   User selection
        print(f"\nSelect the analysis index: ['e' to main menu] ")
        try:
            choice = input()

            print("-" * 100)

            if (choice == 'e'):
                return 'e' # -> to MAIN MENU
                    
            selected_analysis = int(choice)

        except Exception as e:
            print("\tINPUT ERROR: Invalid input")    # Invalid user input
            print(f"\t\t {e}")
            print("-" * 100)
            continue

        else:

            if (selected_analysis not in range(len(analysis_list))):
                print("\tINPUT ERROR: Invalid option -> Out of range!")    # Invalid user choice
                print("-" * 100)
                continue
            
            else:
                return selected_analysis


def perform_analysis(analysis, dataset):

    match analysis:

        # IP BASED
        case 0:
            ip.analysis(dataset, "country")
        case 1:
            ip.analysis(dataset, "continent")
        case 2:
            ip.analysis(dataset, "as_name")
        case 3:
            ip.stability(dataset)

        # PAYLOAD BASED
        case 4:
            payload.analysis(dataset, "Payload Size")
        case 5:
            payload.analysis(dataset, "Most Common")
        case 6:
            payload.analysis(dataset, "Resources Number")
        case 7:
            payload.resources_depth(dataset)
        case 8:
            payload.n_coap_servers(dataset)
        case 9:
            payload.response_code_distribution(dataset)
        case 10:
            payload.payload_attributes_distribution(dataset)      
            

def analysis_menu():

    while(True):

        # 1) analysis selection
        levels = ["ZMAP dataset", "experiment", "date"]
        path = {'phase': 'DataRefinement', 'folder': 'results'}

        for level in levels:
                choice = files_handling.file_selection(level, path)

                if choice == None:
                    return
                else: 
                    path.update({level: choice})
        
        # preloaded dataframe
        data_df = pd.read_csv(files_handling.path_dict_to_str(path))

        # enable multiple analysis on the already selected dataset
        while(True):

            # 2) analysis selection      
            analysis_id = analysis_selection()

            if analysis_id == 'e':
                return

            # 3) perform selected analysis on selected test dataset
            perform_analysis(analysis_id, data_df)