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

        for _, chunk in enumerate(csv_reader):

            # ----------- enrich-chunk -----------
            chunk['observable'] = False

            # ----------- discovery -----------
            print('-' * 100)
            print("\t1. DISCOVERY")
            time.sleep(MENU_WAIT)

            # perform discovery over already found IP addresses
            discovery_df = asyncio.run(coap(chunk, 0))
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/cleaned/{cidr_id}/', None)
            discovery_df = payload_handling.options_to_json(discovery_df)
            
            discovery_df.to_csv(filename, index=False, header=add_header, mode='a')
            
            add_header = False
            
            
            
            
            
    filepath = f'O1_DataCollection/data/observe/{cidr_id}/'
    
    master_observe = os.listdir(filepath)
    master_observe.sort()
    
    filepath += master_observe[0]
    
    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    #   get IP addresses from master ip info file
    with pd.read_csv(filepath, chunksize=CHUNK_SIZE, usecols=['saddr','uri']) as csv_reader:
        
        # when an header is necessary, it must be happended on the first chunk only
        add_observe_header = True

        for _, chunk in enumerate(csv_reader):

            # ----------- get-observable-resources -----------
            print('-' * 100)
            print("\t2. GET OBSERVABLE RESOURCES")
            time.sleep(MENU_WAIT)
            
            if chunk.empty:
                
                print("\tThere were no observable resources")
                
            else:
                
                print(chunk)

                # perform the GET requests to found ZMap resources
                observable_res_df = asyncio.run(coap(chunk, 2))
                filename = workflow_handling.create_file(f'O1_DataCollection/data/observe/{cidr_id}/', None)
                observable_res_df[['saddr', 'uri', 'data', 'data_length']].to_csv(filename, index=False, header=add_observe_header, mode='a')

                add_observe_header = False
                
    return