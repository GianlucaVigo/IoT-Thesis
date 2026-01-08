import asyncio
import pandas as pd
import time

from aiocoap import *

from utils import payload_handling, workflow_handling
from O1_DataCollection.coap import coap

################################################################################################

# constants definition
MENU_WAIT = 0.5
CHUNK_SIZE = 5000

################################################################################################

def lookups(cidr_id):
    
    filename = f'O1_DataCollection/data/discovery/csv/{cidr_id}.csv'

    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    #   get IP addresses from master ip info file
    with pd.read_csv(filename, chunksize=CHUNK_SIZE, usecols=['saddr', 'success']) as csv_reader:

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
            discovery_df = asyncio.run(coap(chunk, True))
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/cleaned/{cidr_id}/', None)
            discovery_df = payload_handling.options_to_json(discovery_df)
            
            discovery_df.to_csv(filename, index=False, header=add_header, mode='a')
            
            # insert 'success' column and set it to 1 in order to pass the success check
            #   -> I rely on the remaining checks
            discovery_df['success'] = 1

            # ----------- get-resources -----------
            print('-' * 100)
            print("\t2. GET RESOURCES")
            time.sleep(MENU_WAIT)

            # perform the GET requests to found ZMap resources
            get_resources_df = asyncio.run(coap(discovery_df[['saddr','code','success','data','options']], False))
            filename = workflow_handling.create_file(f'O1_DataCollection/data/get/{cidr_id}/', None)
            payload_handling.options_to_json(get_resources_df).to_csv(filename, index=False, header=add_header, mode='a')

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
                filename = workflow_handling.create_file(f'O1_DataCollection/data/observe/{cidr_id}/', None)
                observable_resources_df[['saddr', 'uri', 'data', 'data_length']].to_csv(filename, index=False, header=add_observe_header, mode='a')
                print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'data', 'data_length']]}")

                add_observe_header = True

            add_header = False

    return