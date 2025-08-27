import asyncio

from DataRefinement import coap, ip
from utils import files_handling

        
def refinement_menu(mode):

    match(mode):

        # COAP menu
        case "coap":
            
            '''1) Partition selection'''
            # Levels:
            #   0. zmap dataset (ex. 01_output.csv)
            #   1. experiment (ex. exp_0)
            #   2. partition (ex. 3_5.csv)

            levels = ['ZMAP dataset', 'experiment', 'partition']
            path = {'phase': 'IpScan', 'folder': 'results_partitioned'}

            for level in levels:
                choice = files_handling.file_selection(level, path)

                if choice == None:
                    return
                else: 
                    path.update({level: choice})


            # 2) Find CoAP server
            '''
            EVENT LOOP: 
            - the core that manages and distributes tasks
            - like a central hub with tasks circling around it waiting for their turn to be executed
            -- each task takes its turn in the center where it's either
            --- EXECUTED immediately
            --- PAUSED if it's waiting for something
            ---- when a tasks awaits it steps aside making room for another task to run ensuring the loop is always efficiently utilized
            ---- once the awaited operation is complete the task will resume ensuring a smooth and responsive program flow
            '''

            asyncio.run(coap.findCoapServers(path))


        # IP menu
        case "ip":

            # 1) Partition selection
            # Levels:
            #   0. zmap dataset (ex. 01_output.csv)
            #   1. experiment (ex. exp_0)
            #   2. date (ex. 2025-08-18)

            levels = ['ZMAP dataset', 'experiment', 'date']
            path = {'phase': 'DataRefinement', 'folder': 'results_partitioned'}

            for level in levels:
                choice = files_handling.file_selection(level, path)

                if choice == None:
                    return
                else: 
                    path.update({level: choice})


            # 2) check if all partitions are available
            #   ALL available
            if files_handling.all_partitions_done(path):
                
                # 3) find IP info
                ip.findIpInfo(path)

            #   NOT all available
            else:
                print("Some partition has not been processed yet: please perform operation 1. in the main menu for each defined partition.")
                print("-" * 100)
            

    return None