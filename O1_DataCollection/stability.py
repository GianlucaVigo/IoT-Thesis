import asyncio
import pandas as pd
import time
import os

from aiocoap import *

from utils import payload_handling, workflow_handling
from O1_DataCollection.coap import coap

################################################################################################

# constants definition
MENU_WAIT = 0.5
CHUNK_SIZE = 10000

################################################################################################

'''NEW INTERNET WIDE SEARCH'''
'''-------------------------------------------------------'''

def lookups():

    # Check if ZMap raw results are available
    zmap_raw_results_path = "O1_DataHandling/discovery/cleaned/"

    zmap_raw_results_portions = os.listdir(zmap_raw_results_path)
    zmap_raw_results_portions.sort()

    # empty list
    if not zmap_raw_results_portions:
        
        print("[ERROR] You must execute ZMap before considering this phase!")
        
        return

    ####################################################

    # update zmap raw results path including portion file name
    zmap_raw_results_path += zmap_raw_results_portions[0]

    # [0.csv] -> list with at most one item
    # [0] -> get the first and only one element
    # [0] -> access the first char
    try:
        cidr_id = int(zmap_raw_results_portions[0][0])

    except Exception as e:
        print(e)

    ####################################################

    print('=' * 75)
    print("--------- New Lookup ---------")
    
    '''1) FILES CREATION'''
    '''-------------------------------------------------------'''    
    # create a new empty csv file in all the previously elencated directories

    output_paths = workflow_handling.create_today_files(cidr_id, False)

    '''2) LOOKUP PHASE'''
    '''-------------------------------------------------------'''
    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    

    with pd.read_csv(zmap_raw_results_path, chunksize=CHUNK_SIZE, usecols=['saddr']) as csv_reader:

        # when an header is necessary, it must be happended on the first chunk only
        add_header = True
        add_observe_header = True

        for i, chunk in enumerate(csv_reader):

            # ----------- chunk-info -----------
            print(f"\tChunk nr [{i+1}]")

            # ----------- enrich-chunk -----------
            chunk['observable'] = False

            # ----------- discovery -----------
            print('-' * 100)
            print("\t1. DISCOVERY")
            time.sleep(MENU_WAIT)

            # perform discovery over already found IP addresses
            discovery_df = asyncio.run(coap(chunk[['saddr']], True))
            discovery_df.to_csv(output_paths[0], index=False, header=add_header, mode='a')

            # ----------- get-resources -----------
            print('-' * 100)
            print("\t2. GET RESOURCES")
            time.sleep(MENU_WAIT)

            # perform the GET requests to found ZMap resources
            get_resources_df = asyncio.run(coap(discovery_df[['saddr','code','success','data','options']], False))

            payload_handling.options_to_json(get_resources_df).to_csv(output_paths[1], index=False, header=add_header, mode='a')

            # ----------- observe -----------
            print('-' * 100)
            print("\t3. OBSERVE RESOURCES")
            time.sleep(MENU_WAIT)

            # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
            observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]

            if observable_resources_df.empty:
                
                print("\t\tThere were NOT observable resources within the collected dataset")

            else:
                
                # store essential data
                observable_resources_df[['saddr', 'uri', 'data', 'data_length']].to_csv(output_paths[2], index=False, header=add_observe_header, mode='a')
                print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'data', 'data_length']]}")

                add_observe_header = True

            add_header = False

    return