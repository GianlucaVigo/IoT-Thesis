import asyncio
import pandas as pd
import time
import datetime

from aiocoap import *

from utils import payload_handling, workflow_handling, zmap_handling
from O1a_DataCollection.coap import get_requests

################################################################################################

# constants definition

MENU_WAIT = 0.5
CHUNK_SIZE = 10000

################################################################################################

'''NEW INTERNET WIDE SEARCH'''
'''-------------------------------------------------------'''
def lookups(cidr_id, zmap_user_cmd, approach):
    
    current_date = str(datetime.date.today())

    print('=' * 75)
    print(f"[{current_date}] New Internet Wide Search")
    print("Perform a complete new Internet discovery related to the portion selected!")

    '''1) FILES CREATION'''
    '''-------------------------------------------------------'''
    # CSV output files creation
    output_paths = [
        # 0) Raw ZMap csv file
        f'O1a_DataCollection/discovery/csv/{cidr_id}/',
        # 1) Cleaned/Decoded csv file         
        f'O1a_DataCollection/discovery/cleaned/{cidr_id}/',
        # 2) Ip Info csv file                            
        f'O1a_DataCollection/discovery/ip_info/{cidr_id}/',
        # 3) Get Resources
        f'O1a_DataCollection/get/{cidr_id}/',
        # 4) Observe
        f'O1a_DataCollection/observe/{cidr_id}/'                               
    ]                 

    # create a new empty csv file in all the previously elencated directories
    workflow_handling.create_today_files(output_paths, current_date)
    
    '''2) ZMAP DISCOVERY'''
    '''-------------------------------------------------------'''
    print("++++++++++++++++++++++++++++++++++")
    print("+++++++++ CoAP DISCOVERY +++++++++")
    print("++++++++++++++++++++++++++++++++++")

    # DISCOVERY: ZMap command additions
    cmd_additions = [
        "--probe-args=file:utils/zmap/examples/udp-probes/coap_5683.pkt",
        "--output-file=" + output_paths[0] + f"{current_date}.csv" 
    ]

    # enrich the base ZMap command with the user specified options
    cmd_additions.extend(zmap_user_cmd)

    zmap_handling.execute_zmap(cmd_additions)

    '''3) ELABORATE ZMAP RESULTS'''
    '''-------------------------------------------------------'''
    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    with pd.read_csv(output_paths[0] + f"{datetime.date.today()}.csv", chunksize=CHUNK_SIZE) as csv_reader:

        # when an header is necessary, it must be happended on the first chunk only
        add_header = True
        add_observe_header = True

        for i, chunk in enumerate(csv_reader):

            # ----------- chunk-info -----------
            print(f"\tChunk nr [{i+1}]")

            # ----------- raw-master-dataset -----------
            print('-' * 100)
            print("\t ZMAP RAW DATASET")
            # print number of entries in the current chunk
            print(f"\t\tNumber of entries: {chunk.shape[0]}")

            # ----------- remove-duplicates -----------                 
            print('-' * 100)
            print("\t DUPLICATES REMOVAL")
            time.sleep(MENU_WAIT)
            # clean the chunk by removing eventual duplicates -> 'probes' option
            chunk = workflow_handling.remove_duplicates(chunk)
            print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

            # ----------- enrich-chunk -----------
            chunk['observable'] = False

            # ----------- decode-zmap-payload -----------
            print('-' * 100)
            print("\t ZMAP BINARY DECODE")
            time.sleep(MENU_WAIT)
            # decode the ZMap results
            decode_res = asyncio.run(workflow_handling.decode(chunk,'/.well-known/core', approach))
            chunk = decode_res[0]
            print(f"\n\t\t{decode_res[1]}")

            # ----------- store-discovery-dataframe -----------
            print('-' * 100)
            print("\t DISCOVERY DATASET STORAGE")
            time.sleep(MENU_WAIT)
            # stored the cleaned chunk version in append mode
            payload_handling.options_to_json(chunk).to_csv(output_paths[1] + f"{current_date}.csv", index=False, header=add_header, mode='a')
            print("\t\tCleaned version stored correctly!")

            # ----------- ip-info -----------
            print('-' * 100)
            print("\t ADDITIONAL IP INFORMATION EXTRACTION")
            time.sleep(MENU_WAIT)
            # extract and store the IP addresses collected by ZMap processing
            ip_info_df = workflow_handling.extract_ip_info(chunk[['saddr']])
            ip_info_df.to_csv(output_paths[2] + f"{current_date}.csv", index=False, header=add_header, mode='a')

            # ----------- get-resources -----------
            print('-' * 100)
            print("\t7. GET RESOURCES")
            time.sleep(MENU_WAIT)
            # perform the GET requests to found ZMap resources
            get_resources_df = asyncio.run(get_requests(chunk[['saddr','code','success','data','options']]))
            payload_handling.options_to_json(get_resources_df).to_csv(output_paths[3] + f"{current_date}.csv", index=False, header=add_header, mode='a')

            # ----------- observe -----------
            print('-' * 100)
            print("\t8. OBSERVE RESOURCES")
            time.sleep(MENU_WAIT)
            # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
            observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]

            if observable_resources_df.empty:
                print("\t\tThere were NOT observable resources within the collected dataset")
            else:
                # store essential data
                observable_resources_df[['saddr', 'uri', 'data', 'data_length']].to_csv(output_paths[4] + f"{current_date}.csv", index=False, header=add_observe_header, mode='a')
                print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'data', 'data_length']]}")
                add_observe_header = True
                
            add_header = False

    return