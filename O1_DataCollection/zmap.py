import pandas as pd
import time
import asyncio
import datetime
import os

from utils import payload_handling, workflow_handling
from O1_DataCollection.coap import coap
from O1_DataCollection.lookups import lookups

################################################################################################

# constants definition
MENU_WAIT = 0.5
CHUNK_SIZE = 1000

################################################################################################

# before refining the zmap results, balance them into 7 datasets with evenly distributed num of rows
def balance_zmap_datasets():
    
    # 1. Read and combine all CSVs
    path_to_zmap_datasets = 'O1_DataCollection/data/discovery/csv/'
    
    zmap_datasets = os.listdir(path_to_zmap_datasets)
    zmap_datasets.sort()
    
    zmap_dataframes = [pd.read_csv(path_to_zmap_datasets + f) for f in zmap_datasets]
    
    all_data = pd.concat(zmap_dataframes, ignore_index=True)
    
    
    # 2. Compute how many rows each output file should have
    final_num_files = 7
    
    total_rows = len(all_data)
    
    base_rows = total_rows // final_num_files
    extra_rows = total_rows % final_num_files
    
    
    # 3. Split evenly and write new CSV files
    start = 0
    
    for i in range(final_num_files):
        rows = base_rows + (1 if i < extra_rows else 0)
        chunk = all_data.iloc[start:start + rows]
        chunk.to_csv(f'{path_to_zmap_datasets}{i}.csv', index=False)
        start += rows

################################################################################################

def elaborate_zmap_results(cidr_id):
    
    date_and_time = datetime.datetime.today()

    '''ELABORATE ZMAP RESULTS'''
    '''-------------------------------------------------------'''
    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    filename = f'O1_DataCollection/data/discovery/csv/{cidr_id}.csv'
    
    with pd.read_csv(filename, chunksize=CHUNK_SIZE) as csv_reader:

        # when an header is necessary, it must be happended on the first chunk only
        add_header = True
        add_observe_header = True
        add_undecodable_msgs_header = True

        for i, chunk in enumerate(csv_reader):

            # ----------- chunk-info -----------
            print(f"\tChunk nr [{i+1}]")

            # ----------- raw-master-dataset -----------
            print('-' * 50)
            print("\tZMAP RAW DATASET")
            time.sleep(MENU_WAIT)
            # print number of entries in the current chunk
            print(f"\tNumber of entries: {chunk.shape[0]}")
            
            # ----------- remove-invalid-ips -----------
            print('-' * 50)
            print("\tINVALID IP ADDRESSES REMOVAL")
            time.sleep(MENU_WAIT)
            # remove rows with empty 'saddr' field
            chunk.dropna(subset=['saddr'], inplace=True)
            print(f"\tNumber of unique entries with valid ip address: {chunk.shape[0]}")

            # ----------- remove-duplicates -----------                 
            print('-' * 50)
            print("\tDUPLICATES REMOVAL")
            time.sleep(MENU_WAIT)
            # clean the chunk by removing eventual ICMP duplicates -> 'probes' + 'output-filter' options
            chunk = workflow_handling.remove_duplicates(chunk)
            print(f"\tNumber of unique entries: {chunk.shape[0]}")

            # ----------- enrich-chunk -----------
            chunk['observable'] = False
            
            # ----------- decode-zmap-payload -----------
            print('-' * 50)
            print("\tZMAP BINARY DECODE")
            time.sleep(MENU_WAIT)
            # decode the ZMap results
            decode_res = asyncio.run(workflow_handling.decode(chunk,'/.well-known/core'))
            chunk = decode_res[0]
            
            print('-' * 50)
            print("\tDecode Results")
            for option, count in decode_res[1].items():
                print(f"\t\t{option} - {count}")
            
            if decode_res[2] is not None:
                print('-' * 50)
                print("\tUndecodable Messages")
                print(decode_res[2])
                
                filename = workflow_handling.create_file('O1_DataCollection/data/discovery/undecodable_msgs/', cidr_id, add_undecodable_msgs_header, date_and_time)
                decode_res[2].to_csv(filename, index=False, header=add_undecodable_msgs_header, mode='a')
                add_undecodable_msgs_header = False

            # ----------- store-discovery-dataframe -----------
            print('-' * 50)
            print("\tDISCOVERY DATASET STORAGE")
            time.sleep(MENU_WAIT)
            # stored the cleaned chunk version in append mode
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/cleaned/{cidr_id}/', None, add_header, date_and_time)
            payload_handling.options_to_json(chunk).to_csv(filename, index=False, header=add_header, mode='a')
            print("\tCleaned version stored correctly!")

            # ----------- ip-info -----------
            print('-' * 50)
            print("\tADDITIONAL IP INFORMATION EXTRACTION")
            time.sleep(MENU_WAIT)
            # extract and store the IP addresses collected by ZMap processing 
            ip_info_df = workflow_handling.extract_ip_info(chunk[['saddr']])
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/ip_info/{cidr_id}', None, add_header, date_and_time)
            ip_info_df.to_csv(filename, index=False, header=add_header, mode='a')

            # ----------- get-resources -----------
            print('-' * 50)
            print("\tGET RESOURCES")
            time.sleep(MENU_WAIT)
            # perform the GET requests to found ZMap resources
            get_resources_df = asyncio.run(coap(chunk[['saddr','code','data','options']], 1))
            filename = workflow_handling.create_file('O1_DataCollection/data/get/', cidr_id, add_header, date_and_time)
            payload_handling.options_to_json(get_resources_df).to_csv(filename, index=False, header=add_header, mode='a')

            # ----------- observe -----------
            print('-' * 50)
            print("\tOBSERVE RESOURCES")
            time.sleep(MENU_WAIT)
            # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
            observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]

            if observable_resources_df.empty:
                print("\t\tThere were NOT observable resources within the collected dataset")
            else:
                # store essential data
                filename = workflow_handling.create_file(f'O1_DataCollection/data/observe/{cidr_id}/', None, add_observe_header, date_and_time)
                observable_resources_df[['saddr', 'uri', 'data', 'data_length', 'observable']].to_csv(filename, index=False, header=add_observe_header, mode='a')
                print(f"\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'data', 'data_length', 'observable']]}")
                add_observe_header = False
                
            add_header = False
    
    return

################################################################################################

def after_zmap_execution():

    try:
        
        portion_id = portion_selection()
        
        # ------------------------------------------------
        
        start_get = datetime.datetime.now()
        
        elaborate_zmap_results(portion_id)
        
        elapsed_get = (datetime.datetime.now() - start_get).total_seconds()
        
        print(f"\t{portion_id} Time needed to accomplish GET requests: {elapsed_get}")
        
        # ------------------------------------------------
        
        SECONDS_PER_DAY = 24*60*60
    
        for i, offset in enumerate([1, 2, 3, 4, 5, 6], start=1):
            delay = (datetime.timedelta(seconds=offset * SECONDS_PER_DAY) - (datetime.datetime.now() - start_get)).total_seconds()
            delay = max(0, delay)  # prevent negative delays
            
            print('-'*30)
            print(f"[{i}] Waiting for {delay} seconds ({datetime.datetime.now()})")
            print('-'*30)

            time.sleep(delay)
            
            start_lookup = datetime.datetime.now()
            
            lookups(portion_id)
            
            elapsed_lookup = (datetime.datetime.now() - start_lookup).total_seconds()
        
            print(f"\t{portion_id} Time needed to accomplish LOOKUP: {elapsed_lookup}")
    
    except Exception as e:
        # print the error type
        print(e)

    return

################################################################################################

def portion_selection():

    print('-' * 30)
    print("From which portion do you want to start?\n")

    # header
    print("\tindex".ljust(10))
    # options
    for index in range(7):

        print(f"\t{index}".ljust(10))
    
    # user selects the portion id
    print("\nPlease select the index:", end="")
    
    try:
        
        # get the index and convert it to integer
        portion_id = int(input())
        
    except Exception as e:
        return e
    
    return portion_id