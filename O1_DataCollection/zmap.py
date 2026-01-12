import pandas as pd
import time
import asyncio
import subprocess
import datetime
import os

from utils import payload_handling, workflow_handling
from O1_DataCollection.coap import coap
from O1_DataCollection.lookups import lookups

################################################################################################

# constants definition
MENU_WAIT = 0.5
CHUNK_SIZE = 5000

################################################################################################

def balance_zmap_datasets():
    
    # 1. Read and combine all CSVs
    path_to_zmap_datasets = 'O1_DataCollection/data/discovery/csv/'
    
    zmap_datasets = os.listdir(path_to_zmap_datasets)
    
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

def zmap():
    
    print('-' * 50, '[ZMAP]', '-' * 50)
    
    balance_zmap_datasets()
    
    return
    
    cidr_id, cidr = before_zmap_execution()
    
    execute_zmap(cidr_id, cidr)
    
    return

################################################################################################

def elaborate_zmap_results(cidr_id):

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
            print('-' * 100)
            print("\tZMAP RAW DATASET")
            # print number of entries in the current chunk
            print(f"\t\tNumber of entries: {chunk.shape[0]}")

            # ----------- remove-duplicates -----------                 
            print('-' * 100)
            print("\tDUPLICATES REMOVAL")
            time.sleep(MENU_WAIT)
            # clean the chunk by removing eventual duplicates -> 'probes' option
            chunk = workflow_handling.remove_duplicates(chunk)
            print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

            # ----------- enrich-chunk -----------
            chunk['observable'] = False
             
            # ----------- ip-info -----------
            print('-' * 100)
            print("\tADDITIONAL IP INFORMATION EXTRACTION")
            time.sleep(MENU_WAIT)
            # extract and store the IP addresses collected by ZMap processing 
            ip_info_df = workflow_handling.extract_ip_info(chunk[['saddr']])
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/ip_info/', cidr_id)
            ip_info_df.to_csv(filename, index=False, header=add_header, mode='a')
            
            # ----------- decode-zmap-payload -----------
            print('-' * 100)
            print("\tZMAP BINARY DECODE")
            time.sleep(MENU_WAIT)
            # decode the ZMap results
            decode_res = asyncio.run(workflow_handling.decode(chunk,'/.well-known/core'))
            chunk = decode_res[0]
            
            print("\tDecode Results")
            print(f"\t\t{decode_res[1]}")
            
            if decode_res[2] is not None:
                print("\tUndecodable Messages")
                print(decode_res[2])
                
                filename = workflow_handling.create_file('O1_DataCollection/data/discovery/undecodable_msgs/', cidr_id)
                decode_res[2].to_csv(filename, index=False, header=add_undecodable_msgs_header, mode='a')
                add_undecodable_msgs_header = False

            # ----------- store-discovery-dataframe -----------
            print('-' * 100)
            print("\tDISCOVERY DATASET STORAGE")
            time.sleep(MENU_WAIT)
            # stored the cleaned chunk version in append mode
            filename = workflow_handling.create_file(f'O1_DataCollection/data/discovery/cleaned/{cidr_id}/', None)
            payload_handling.options_to_json(chunk).to_csv(filename, index=False, header=add_header, mode='a')
            print("\t\tCleaned version stored correctly!")

            # ----------- get-resources -----------
            print('-' * 100)
            print("\tGET RESOURCES")
            time.sleep(MENU_WAIT)
            # perform the GET requests to found ZMap resources
            get_resources_df = asyncio.run(coap(chunk[['saddr','code','success','data','options']], 1))
            filename = workflow_handling.create_file('O1_DataCollection/data/get/', cidr_id)
            payload_handling.options_to_json(get_resources_df).to_csv(filename, index=False, header=add_header, mode='a')

            # ----------- observe -----------
            print('-' * 100)
            print("\tOBSERVE RESOURCES")
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

################################################################################################

def after_zmap_execution():

    try:
        cidr_id, cidr = portion_selection()
        
        start = datetime.datetime.now()
        
        elaborate_zmap_results(cidr_id)
        
        SECONDS_PER_DAY = 5*60
    
        for i, offset in enumerate([1, 2], start=1):
            delay = (datetime.timedelta(seconds=offset * SECONDS_PER_DAY) - (datetime.datetime.now() - start)).total_seconds()
            delay = max(0, delay)  # prevent negative delays
            
            print('-'*30)
            print(f"[{i}] Waiting for {delay} seconds ({datetime.datetime.now()})")
            print('-'*30)

            time.sleep(delay)
            
            lookups(cidr_id)
    
    except Exception as e:
        # print the error type
        print(e)

    return

################################################################################################

def execute_zmap(cidr_id, cidr):

    # base command to be executed
    command = [
        "sudo",
        "zmap"
    ]

    config_option = "--config=utils/zmap_configs/config.txt"
    output_file = f"--output-file=O1_DataCollection/data/discovery/csv/{cidr_id}.csv"
    command.extend([config_option, output_file, cidr])
        
    # ZMap Command Execution
    # stdout=subprocess.PIPE    => captures the process's std output so that Python can read it
    # stderr=subprocess.STDOUT  => redirect the std error to output and so to Python
    # text=True                 => convert the std output into text (instead of bytes)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    try:
        # read the process output line-by-line as it runs
        while True:
            # blocks until a newline is available or EOF occurs
            line = p.stdout.readline()

            # line is empty/no data/EOF
            if not line and p.poll() is not None:
                break # exit from the loop
            # otherwise print the captured line
            if line:
                print("[zmap]", line.rstrip())

    finally:
        # close the captured stdout file object 
        p.stdout.close()
        # let the process ends and collect its return code
        ret = p.wait()
        # print return exit
        print("ZMap exit:", ret)

    return

################################################################################################

def portion_selection():
    
    # NB: 224.0.0.0/3 -> not considered because of the blocklist
    # In my idea one partition for each VM
    internet_portions = ['0.0.0.0/3', '32.0.0.0/3', '64.0.0.0/3', '96.0.0.0/3', '128.0.0.0/3', '160.0.0.0/3', '192.0.0.0/3']

    print("From which portion of the Internet do you want to start?\n")

    # header
    print("\tindex".ljust(10), "cidr".ljust(19))
    # options
    for index, portion in enumerate(internet_portions):

        # build up the info line to be printed
        info_line = "\t"
        info_line += f"{index}".ljust(10)       # add the index
        info_line += f"{portion}".ljust(20)     # add the portion cidr

        print(info_line)
    
    # user selects the portion id
    print("\nPlease select the index:", end="")
    
    try:
        
        # get the index and convert it to integer
        internet_portion_id = int(input())
        # retrieve the cidr related
        internet_portion = internet_portions[internet_portion_id]
        
    except Exception as e:
        return e
    
    return [internet_portion_id, internet_portion]

################################################################################################

def before_zmap_execution():
    
    # 1. choose internet portion
    try:
        internet_portion_id, internet_portion = portion_selection()
    except Exception as e:
        # print the error type
        print(e)
    
    # 2. create the .csv file for zmap output
    # raw csv file path       
    workflow_handling.create_file('O1_DataCollection/data/discovery/csv/', internet_portion_id)
    
    return [internet_portion_id, internet_portion]