import asyncio
import pandas as pd
import time
import os
import datetime

from aiocoap import *

from utils import payload_handling, workflow_handling
from O1_DataCollection.coap import coap

################################################################################################

# constants definition
MENU_WAIT = 0.5
CHUNK_SIZE = 1000

################################################################################################

def lookups(cidr_id):
    
    date_and_time = datetime.datetime.today()
    
    filename_path = f'O1_DataCollection/data/discovery/cleaned/{cidr_id}/'
    
    all_files = os.listdir(filename_path)
    all_files.sort()
    
    # master cleaned csv file
    filename = filename_path + all_files[0]

    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    #   get IP addresses from master ip info file
    with pd.read_csv(filename, chunksize=CHUNK_SIZE, usecols=['saddr']) as csv_reader:

        # when an header is necessary, it must be happended on the first chunk only
        add_header = True

        for i, chunk in enumerate(csv_reader):
            
            # ----------- chunk-info -----------
            print(f"\tChunk nr [{i+1}]")

            # ----------- enrich-chunk -----------
            chunk['observable'] = False

            # ----------- discovery -----------
            print('-' * 50)
            print("\tLOOKUP")
            time.sleep(MENU_WAIT)

            # perform discovery over already found IP addresses
            discovery_df = asyncio.run(coap(chunk, 0))
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/cleaned/{cidr_id}/', None, add_header, date_and_time)
            discovery_df = payload_handling.options_to_json(discovery_df)
            discovery_df.to_csv(filename, index=False, header=add_header, mode='a')
            
            # ----------------------------------  
            # keep only active ip addresses
            print("Before: ", discovery_df.shape[0])
            discovery_df.dropna(subset=['code'], inplace=True)
            print("After: ", discovery_df.shape[0])
            
            # ----------- ip-info -----------
            print('-' * 50)
            print("\tADDITIONAL IP INFORMATION EXTRACTION")
            time.sleep(MENU_WAIT)
            # extract and store the IP addresses collected by ZMap processing 
            ip_info_df = workflow_handling.extract_ip_info(discovery_df[['saddr']])
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/ip_info/{cidr_id}', None, add_header, date_and_time)
            ip_info_df.to_csv(filename, index=False, header=add_header, mode='a')
            
            add_header = False
                   
            
    filepath = f'O1_DataCollection/data/observe/{cidr_id}/'
    
    master_observe = os.listdir(filepath)
    master_observe.sort()
    
    filepath += master_observe[0]
    
    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    #   get IP addresses from master ip info file
    with pd.read_csv(filepath, chunksize=CHUNK_SIZE, usecols=['saddr','uri', 'observable']) as csv_reader:
        
        # when an header is necessary, it must be happended on the first chunk only
        add_observe_header = True

        for _, chunk in enumerate(csv_reader):

            # ----------- get-observable-resources -----------
            print('-' * 50)
            print("\tGET OBSERVABLE RESOURCES")
            time.sleep(MENU_WAIT)
            
            if chunk.empty:
                
                print("\tThere were no observable resources")
                
            else:

                # perform the GET requests to found ZMap resources
                observable_res_df = asyncio.run(coap(chunk, 2))
                filename = workflow_handling.create_file(f'O1_DataCollection/data/observe/{cidr_id}/', None, add_observe_header, date_and_time)
                observable_res_df[['saddr', 'uri', 'data', 'data_length', 'observable']].to_csv(filename, index=False, header=add_observe_header, mode='a')

                add_observe_header = False
                
    return