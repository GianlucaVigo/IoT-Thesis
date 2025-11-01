import asyncio
import pandas as pd
import time
import datetime

from aiocoap import *

from utils import payload_handling, workflow_handling, zmap_handling

################################################################################################

# constants definition

MENU_WAIT = 0.5
CHUNK_SIZE = 10000

################################################################################################

def string_to_hex(input_string):
    # First, encode the string to bytes
    bytes_data = input_string.encode('utf-8')

    # Then, convert bytes to hex
    hex_data = bytes_data.hex()

    return hex_data

################################################################################################

def encode_uri(uri):

    basic_get_message = "40017d706100"

    if uri=="/": # home-path special case -> no Uri-Path option!
        return basic_get_message

    uri_levels = uri.split('/')

    for i, lev in enumerate(uri_levels[1:]):

        # ------- uri-path option DELTA -------
        if i == 0:          # first iteration
            basic_get_message += "5"
        else:               # other iterations
            basic_get_message += "0"

        # option length < 13 -> fit directly
        if len(lev) < 13:
            basic_get_message += f"{len(lev):1x}{string_to_hex(lev)}"
            
        # option length >= 15 -> extended encoding
        else:
            basic_get_message += f"d{len(lev)-13:02x}{string_to_hex(lev)}"

        
    return basic_get_message

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
        f'O1b_DataCollection/discovery/csv/{cidr_id}/',
        # 1) Cleaned/Decoded csv file         
        f'O1b_DataCollection/discovery/cleaned/{cidr_id}/',
        # 2) Ip Info csv file                            
        f'O1b_DataCollection/discovery/ip_info/{cidr_id}/',
        # 3) Get Resources
        f'O1b_DataCollection/get/{cidr_id}/',
        # 4) Observe
        f'O1b_DataCollection/observe/{cidr_id}/'                               
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
    
    
    '''3) ELABORATE DISCOVERY ZMAP RESULTS'''
    '''-------------------------------------------------------'''
    # dictionary with:
    # K: uri
    # V: list of (IPs having the uri + 'obs' metadata)
    resources_and_ips = {}

    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    with pd.read_csv(output_paths[0] + f"{current_date}.csv", chunksize=CHUNK_SIZE) as csv_reader:

        # when an header is necessary, it must be happended on the first chunk only
        add_header = True

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
            print("\t EXTRACTING URI + IPs")
            time.sleep(MENU_WAIT)

            for _,row in chunk[['saddr','code','success','data','options']].iterrows():

                if not workflow_handling.avoid_get(row):

                    # resources = list of uris + metadata
                    resources = payload_handling.resource_list_of(row['data'])
                    
                    home_path_inserted = False
                    # home path resource is not present in the resource list
                    if row['data'].find('</>') == -1:
                        resources.append('</>')
                        home_path_inserted = True

                    for res in resources:

                        uri = res.split(';')[0].strip('<>')
                            
                        # --------- uris-validity-check ---------
                        # 1. uri must start with '/'
                        if not uri.startswith('/'):
                            print(f"{uri} does not respect the correct syntax -> NOT EVALUATED")
                            continue

                        ip = row['saddr']
                        obs = payload_handling.get_metadata_value_of(res, 'obs')
                        user_inserted = False
                        
                        if uri == '/':
                            user_inserted = home_path_inserted

                        if uri in resources_and_ips.keys():
                            resources_and_ips[uri].append([ip,obs,user_inserted])
                        else:
                            resources_and_ips[uri] = [[ip,obs,user_inserted]]
            
        add_header = False


    '''4) ZMAP GET REQUESTS '''
    '''-------------------------------------------------------'''
    print("+++++++++++++++++++++++++++++")
    print("+++++++++ CoAP GETs +++++++++")
    print("+++++++++++++++++++++++++++++")
    
    # supporting files
    # I: allowlist-file -> ip list
    ips_support_file = "O1b_DataCollection/get/ips.csv"
    # O: zmap results of GET requests
    res_support_file = "O1b_DataCollection/get/res.csv"

    add_header = True
    add_observe_header = True
    
    # consider one uri at a time
    for i, uri in enumerate(resources_and_ips.keys()):

        #################################################

        # ZMAP IP ALLOWLIST
        # write the list of IPs into a support file, then used by ZMap
        ip_list = []
            
        for item in resources_and_ips[uri]:
            ip_list.append(item[0]) # append IP

        ips_df = pd.DataFrame(ip_list)
        ips_df.to_csv(ips_support_file, index=False, header=False)

        #################################################

        # ZMAP COMMAND PREPARATION
        # probe hex encoding: incorporate the uri
        probe_encoded = encode_uri(uri)

        print('#' * 75)
        print(f"{i+1}/{len(resources_and_ips.keys())}")
        print(f"\tUri: {uri}")
        print(f"\tIPs and properties: {resources_and_ips[uri]}")
        print(f"\tNum of IPs: {len(resources_and_ips[uri])}")
        print(f"\tProbe: {probe_encoded}")


        # DISCOVERY: ZMap command additions
        cmd = [
            "--allowlist-file=" + ips_support_file,
            "--probe-args=hex:" + probe_encoded,
            "--output-file=" + res_support_file,
            "--cooldown=10",
            "--probes=3"
        ]
            
        zmap_handling.execute_zmap(cmd)

        #################################################
        
        '''5) ELABORATE ZMAP GET RESULTS '''
        '''-------------------------------------------------------'''
        # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
        with pd.read_csv(res_support_file, chunksize=CHUNK_SIZE) as csv_reader:

            for i, chunk in enumerate(csv_reader):

                # ----------- chunk-info -----------
                print(f"\tChunk nr [{i+1}]")
        
                # ----------- raw-dataset -----------
                print('-' * 100)
                print("\t ZMAP GET RAW DATASET")
                print(f"\t\tNumber of entries: {chunk.shape[0]}")

                # ----------- remove-duplicates -----------                
                print('-' * 100)
                print("\t DUPLICATES REMOVAL")
                time.sleep(MENU_WAIT)
                # clean the chunk by removing eventual duplicates -> 'probes' option
                chunk = workflow_handling.remove_duplicates(chunk)
                print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

                # ----------- enrich ZMap result -----------
                for index, ip in enumerate(chunk['saddr']):

                    for data_items in resources_and_ips[uri]:
                            
                        ip_addr = data_items[0]
                        obs = data_items[1]
                        user_inserted = data_items[2]

                        if ip_addr == ip:
                            chunk.loc[index, 'observable'] = obs
                            chunk.loc[index, 'user_inserted'] = user_inserted
                            break
            
                # ----------- decode-zmap-payload -----------
                print('-' * 100)
                print("\t ZMAP BINARY DECODE")
                time.sleep(MENU_WAIT)
                # decode the ZMap results
                decode_res = asyncio.run(workflow_handling.decode(chunk, uri, approach))
                get_resources_df = decode_res[0]
                print(f"\n\t\t{decode_res[1]}")

                # ----------- store-get-dataframe -----------
                print('-' * 100)
                print("\t GET DATASET STORAGE")
                time.sleep(MENU_WAIT)
                # stored the cleaned chunk version in append mode
                payload_handling.options_to_json(get_resources_df).to_csv(output_paths[3]+ f"{current_date}.csv", index=False, header=add_header, mode='a')
                print("\t\tCleaned version stored correctly!")
                        
                # ----------- observe -----------
                print('-' * 100)
                print("\t OBSERVE RESOURCES")
                time.sleep(MENU_WAIT)
                # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
                observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]

                if observable_resources_df.empty:
                    print("\t\tThere were NOT observable resources within the collected dataset")
                else:
                    print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'data', 'data_length']]}")
                    # store essential data
                    observable_resources_df[['saddr', 'uri', 'data', 'data_length']].to_csv(output_paths[4] + f"{current_date}.csv", index=False, header=add_observe_header, mode='a')  
                    add_observe_header = False
                    
                add_header = False
        
    return

