import os
import asyncio
import pandas as pd
import time
import datetime
import subprocess
import maxminddb

from aiocoap import *
from collections import Counter

from utils import payload_handling

################################################################################################

# constants definition

MENU_WAIT = 1
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

# perform the resource discovery operation -> GET (<IP> + <URI>)
async def get(ip_address, res_uri, context):

    decoded_msg = {
        'version': None,
        'mtype': None,
        'token': None,
        'token_length': None,
        'code': None,
        'mid': None,
        'options': None,
        'observable': None,
        'data': None,
        'data_length': None
    }

    print(f"\t\t\tPerforming new GET request to {res_uri} ...")

    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{res_uri}"
    # build the request message
    request = Message(code=GET, uri=uri_to_check)


    try:
        # send the request and obtained the response
        response = await context.request(request).response

    except Exception as e:
        print(f"\t{e}")
        return decoded_msg

    else:

        decoded_msg.update({
            'version': payload_handling.get_version(response),
            'mtype': payload_handling.get_mtype(response),
            'token': payload_handling.get_token(response),
            'token_length': payload_handling.get_token_length(response),
            'code': payload_handling.get_code(response),
            'mid': payload_handling.get_mid(response),
            'options': payload_handling.get_options(response),
            'observable': payload_handling.get_observe(response, res_uri),
            'data': payload_handling.get_payload(response),
            'data_length': payload_handling.get_payload_length(response)
        })

        return decoded_msg # success

################################################################################################
# decode_payload()
#   I: binary payload (in hex)
#   O: structured object with multiple fields
def decode_data(binary_data, uri):

    # default message to be returned (if any error occurs during the decoding process)
    decoded_msg = {
        'version': None,
        'mtype': None,
        'token': None,
        'token_length': None,
        'code': None,
        'mid': None,
        'options': None,
        'observable': None,
        # maintain the raw payload as is
        'data': binary_data,     
        'data_length': len(binary_data)
    }

    # ----- decoding ZMap binary message -----

    try:

        msg = Message.decode(bytes.fromhex(binary_data))

    # unable to decode the message
    except Exception as e:
        print(f'Decoding Error - {e}')
        return decoded_msg
    
    # ----------------------------------------

    # update/populate the starting decoded message structure with all retrieved info/data
    decoded_msg.update({
        'version': payload_handling.get_version(msg),
        'mtype': payload_handling.get_mtype(msg),
        'token': payload_handling.get_token(msg),
        'token_length': payload_handling.get_token_length(msg),
        'code': payload_handling.get_code(msg),
        'mid': payload_handling.get_mid(msg),
        'options': payload_handling.get_options(msg),
        'observable': payload_handling.get_observe(msg, uri),
        'data': payload_handling.get_payload(msg),
        'data_length': payload_handling.get_payload_length(msg)
    })

    return decoded_msg

################################################################################################

# decode()
#   it takes as input the raw/binary ZMap data field and it returns a structured object representing a CoAP response message
#   O: version, message type (mtype), token length, code (response code), mid (message id), tokenn, options, data (payload)
async def decode(df_zmap):

    # it contains all the decoded messages
    new_data_list = []
    # it contains a summary of the decode process (success, unsuccess/<REASON>)
    decode_results = Counter()

    # client context creation for eventual GETs
    context = await Context.create_client_context()

    # iterate over rows
    for _, row in df_zmap.iterrows():

        # default decoded message (it will be returned if any error occurs)
        decoded_msg = {
            'version': None,
            'mtype': None,
            'token': None,
            'token_length': None,
            'code': None,
            'mid': None,
            'options': None,
            'observable': None,
            'data': None,
            'data_length': None
        }

        # if success field is equal to 1 (all UDP kind of result)
        if row['success'] == 1:

            # decoding binary data and get a dictionary as result
            decoded_msg = decode_data(row['data'], row['uri'])

            # print decoded message data field
            print(f"ZMap Payload decoded: {decoded_msg['data']}")

            # if decodede message code field is equal to None -> something went wrong during the decoding process
            #   ex. undecodable message, ...
            if decoded_msg['code'] is None:

                # update summary
                decode_results.update([f"unsuccess/{decoded_msg['data']}"])

            # otherwise it is a success
            else:

                # update summary
                decode_results.update(['success'])

                # detect if ZMap retrieved payload is truncated
                if payload_handling.detect_truncated_response(row['udp_pkt_size'], row['data'], decoded_msg):
                    
                    time.sleep(1)

                    # -> if so, perform a new discovery through aiocoap library
                    decoded_msg = await get(row['saddr'], row['uri'], context)

                    # print the complete/not truncated data/payload field
                    print(f"ZMap Complete Payload decoded: {decoded_msg['data']}")

                # -------- observability handling --------
                # OBS resource 
                if decoded_msg['observable'] == True:

                    #   -> declared OBS = CORRECT              
                    if row['observable'] == True:           
                        decoded_msg['observable'] = 0

                    #   -> NOT declared OBS = WRONG
                    else:                                                   
                        decoded_msg['observable'] = 1

                # NOT OBS resource
                else:

                    #   -> declared OBS = WRONG                                               
                    if row['observable'] == True:                                
                        decoded_msg['observable'] = 2

                    #   -> NOT declared OBS = CORRECT               
                    else:                                                   
                        decoded_msg['observable'] = 3
                # ------------------------------------------

        # if success field is equal to 0 (all icmp, ... kind of result)
        else:
            decode_results.update([f"unsuccess/{row['icmp_unreach_str']}"])

        # each case (un/success) -> append and store it
        new_data_list.append(decoded_msg)
        print('£' * 50)
    
    # close context/UDP socket
    await context.shutdown()

    # Build dataframe from list
    columns = ['version', 'mtype', 'token', 'token_length', 'code', 'mid', 'options', 'data', 'data_length', 'observable']
    for col in columns:
        df_zmap[col] = [d[col] for d in new_data_list]

    # return datafram structure + summary
    return [df_zmap, decode_results]


################################################################################################

# remove_duplicates()
#   it is able to identify duplicates in the ZMap scan csv files: keep the first, discard the others
#   duplicate = both IP address and data fields equal
def remove_duplicates(df_zmap):

    # [subset=['saddr']] Remove duplicates considering 'saddr' field
    # [keep='first'] Keep the first instance (not delete all duplicates)
    # [inplace=True] Whether to modify the DataFrame rather than creating a new one.
    df_zmap.drop_duplicates(subset=['saddr'], keep='first', inplace=True)

    # [inplace=True] Whether to modify the DataFrame rather than creating a new one.
    # [drop=True] Do not try to insert index into dataframe columns. This resets the index to the default integer index.
    df_zmap.reset_index(inplace=True, drop=True)

    return df_zmap

################################################################################################

# extract_ip_info()
#   it is able to retrieve IP related information as country, continent, ...
#   it returns a flat dataframe that will be then stored safely
def extract_ip_info(ip_list_df):

    with maxminddb.open_database('utils/ipinfo/ipinfo_lite.mmdb') as reader:
        # apply -> returns a dictionary of IP related fields
        ip_info = ip_list_df['saddr'].apply(reader.get)
        
        reader.close()

    # normalize/flatten the dictionary
    ip_info_df = pd.json_normalize(ip_info)
        
    # concatenate IP + IP info
    ip_info_df = pd.concat([ip_list_df.reset_index(drop=True), ip_info_df], axis=1)

    return ip_info_df

################################################################################################

def avoid_get(row):

    # When can GET requests be avoided?

    # ----- SUCCESS check -----
    if row['success'] == 0: # success = 0/1
        print(f"\t\tNot successful response to resource discovery: \n\t\tskip {row['saddr']}")
        return True

    # ----- CODE check -----
    if row['code'] != '2.05 Content': # not successful discovery (= 2.05 Content)
        print(f"\t\tNot '2.05 Content' discovery: \n\t\tskip {row['saddr']}")
        return True

    # ----- OPTION check -----
    # Assumption: /.well-known/core resource use the CONTENT FORMAT option equal to LINK FORMAT
    # NB: I filter out everything that is not in LINKFORMAT style (TEXT, ...)
    if row['options'] != None:

        options_dict = eval(row['options'])

        if 'CONTENT_FORMAT' in options_dict.keys():
            if options_dict['CONTENT_FORMAT'] != 'LINKFORMAT':
                return True

    # ----- EMPTY PAYLOAD check -----
    if len(row['data']) == 0: # empty payload string
        print(f"\t\tEmpty payload was returned from resource discovery: \n\t\tskip {row['saddr']}")
        return True

    # ----- STILL BINARY PAYLOAD check -----
    if isinstance(row['data'], bytes):
        print(f"\t\tCan't decode its binary payload: \n\t\tskip {row['saddr']}")
        return True

    return False

################################################################################################

def execute_zmap(command_additions):

    command = [
        "zmap",
        "--target-port=5683",
        "--probe-module=udp",
        "--blocklist-file=utils/zmap/conf/blocklist.conf",
        "--output-module=csv",
        "--output-fields=*",
        "--output-filter=\"repeat=0\"",
        "-q"
    ]

    command.extend(command_additions)

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


'''NEW INTERNET WIDE SEARCH'''
'''-------------------------------------------------------'''
def lookups(cidr, cidr_id, zmap_user_cmd):

    base_path = f'O1b_DataCollection/discovery/csv/{cidr_id}/'

    # list of directories related to eventual dates already analysed
    dates = os.listdir(base_path)

    # if no previous dates -> first time of execution of the program -> return and get NEW/FRESH data
    if not dates:
        print("[NB] No historical data recorded!")
    else:
        dates.sorted()
    # append today date    
    dates.append(str(datetime.date.today()))

    is_new_wide_search = False

    for date in dates:
        
        print('=' * 75)
        if date == str(datetime.date.today()):
            print(f"[{date}] New Internet Wide Search")
            print("Perform a complete new Internet discovery related to the portion selected -> FRESH/NEW DATA")
            is_new_wide_search = True
        else:
            print(f"[{date}] Stability Lookups")
            print("Perform a stability lookup over already found results -> CHECK RESOURCES STABILITY")
            zmap_user_cmd = zmap_user_cmd[:-1]
            # get the IPs to check from a previously created csv file
            ip_list_path = f'O1b_DataCollection/discovery/ip_lists/{cidr_id}/{date}.csv'


        '''1) FILES CREATION'''
        '''-------------------------------------------------------'''
        # CSV output files creation
        output_paths = [
            # 0) Raw ZMap csv file
            f'O1b_DataCollection/discovery/csv/{cidr_id}/{date}/',
            # 1) Cleaned/Decoded csv file         
            f'O1b_DataCollection/discovery/cleaned/{cidr_id}/{date}/',
            # 2) Get Resources
            f'O1b_DataCollection/get/{cidr_id}/{date}/',
            # 3) Observe
            f'O1b_DataCollection/observe/{cidr_id}/{date}/'                               
        ]

        if is_new_wide_search:
            output_paths.extend([
                # 4) Ip List csv file     
                f'O1b_DataCollection/discovery/ip_lists/{cidr_id}/',
                # 5) Ip Info csv file                            
                f'O1b_DataCollection/discovery/ip_info/{cidr_id}/'
            ])
        
        # create a new empty csv file in all the previously elencated directories
        for file_path in output_paths:

            if not os.path.exists(file_path):
                os.makedirs(file_path)

            file_path += f"{datetime.date.today()}.csv"

            with open(file_path, 'w') as new_csv:
                pass


        '''2) ZMAP DISCOVERY'''
        '''-------------------------------------------------------'''
        print("+++++++++ CoAP DISCOVERY +++++++++")

        # DISCOVERY: ZMap command additions
        cmd_additions = [
            "--probe-args=file:utils/zmap/examples/udp-probes/coap_5683.pkt",
            "--output-file=" + output_paths[0] + f"{datetime.date.today()}.csv",  
        ]

        if not is_new_wide_search:
            cmd_additions.append("--allowlist-file=" + ip_list_path)

        # enrich the base ZMap command with the user specified options
        cmd_additions.extend(zmap_user_cmd)
        cmd_additions.append(cidr)

        execute_zmap(cmd_additions)
    
    
        '''3) ELABORATE DISCOVERY ZMAP RESULTS'''
        '''-------------------------------------------------------'''
        # dictionary with:
        # K: uri
        # V: list of (IPs having the uri + 'obs' metadata)
        resources_and_ips = {}

        # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
        with pd.read_csv(output_paths[0] + f"{datetime.date.today()}.csv", chunksize=CHUNK_SIZE) as csv_reader:

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

                # ----------- remove-duplicates -----------     => there could be a problem when dealing with a lot of data (multiple chunks) + multiple probes                                    
                print('-' * 100)
                print("\t DUPLICATES REMOVAL")
                time.sleep(MENU_WAIT)
                # clean the chunk by removing eventual duplicates -> 'probes' option
                chunk = remove_duplicates(chunk)
                print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

                # ----------- enrich ZMap result -----------
                chunk['uri'] = '/.well-known/core'
                chunk['observable'] = None

                # ----------- decode-zmap-payload -----------
                print('-' * 100)
                print("\t ZMAP BINARY DECODE")
                time.sleep(MENU_WAIT)
                # decode the ZMap results
                decode_res = asyncio.run(decode(chunk))
                chunk = decode_res[0]
                print(f"\n\t\t{decode_res[1]}")

                # ----------- store-discovery-dataframe -----------
                print('-' * 100)
                print("\t DISCOVERY DATASET STORAGE")
                time.sleep(MENU_WAIT)
                # stored the cleaned chunk version in append mode
                payload_handling.options_to_json(chunk).to_csv(output_paths[1] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')
                print("\t\tCleaned version stored correctly!")

                if is_new_wide_search:

                    # ----------- ip-list -----------
                    print('-' * 100)
                    print("\t EXTRACTING IP LIST")
                    time.sleep(MENU_WAIT)
                    chunk[['saddr']].to_csv(output_paths[4] + f"{datetime.date.today()}.csv", index=False, header=False, mode='a')

                    # ----------- ip-info -----------
                    print('-' * 100)
                    print("\t ADDITIONAL IP INFORMATION EXTRACTION")
                    time.sleep(MENU_WAIT)
                    # extract and store the IP addresses collected by ZMap processing
                    ip_info_df = extract_ip_info(chunk[['saddr']])
                    ip_info_df.to_csv(output_paths[5] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')

                # ----------- get-resources -----------
                print('-' * 100)
                print("\t EXTRACTING IP + URI")
                time.sleep(MENU_WAIT)

                for _,row in chunk[['saddr','code','success','data','options']].iterrows():

                    if not avoid_get(row):

                        # resources = list of uris + metadata
                        resources = payload_handling.resource_list_of(row['data'])

                        for res in resources:

                            uri = res.split(';')[0].strip('<>')
                            
                            # --------- uris-validity-check ---------
                            # 1. uri must start with '/'
                            if not uri.startswith('/'):
                                continue

                            ip = row['saddr']
                            obs = payload_handling.get_metadata_value_of(res, 'obs')

                            if uri in resources_and_ips.keys():
                                resources_and_ips[uri].append([ip,obs])
                            else:
                                resources_and_ips[uri] = [[ip,obs]]
            
            add_header = False


        '''4) ZMAP GET REQUESTS '''
        '''-------------------------------------------------------'''
        print("+++++++++ CoAP GETs +++++++++")
        # supporting files
        ips_support_file = "O1b_DataCollection/get/ips.csv"
        res_support_file = "O1b_DataCollection/get/res.csv"

        add_header = True
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
            print(f"\tNum of IPs: {len(resources_and_ips[uri])}")
            print(f"\tProbe: {probe_encoded}")


            # DISCOVERY: ZMap command additions
            cmd = [
                "--allowlist-file=" + ips_support_file,
                "--probe-args=hex:" + probe_encoded,
                "--output-file=" + res_support_file,
                "--cooldown=21",
                "--probes=3"
            ]
            
            execute_zmap(cmd)

            #################################################

            '''5) ELABORATE ZMAP GET RESULTS '''
            '''-------------------------------------------------------'''
            # ----------- raw-master-dataset -----------
            print('-' * 100)
            print("\t ZMAP GET RAW DATASET")
            # temporary ZMap get results are store in res_support_file -> convert it to a dataframe
            try:
                zmap_uri_res = pd.read_csv(res_support_file)
            except pd.io.common.EmptyDataError:
                print(f"Empty Result - {uri}")
                continue
            # print number of entries in the current chunk
            print(f"\t\tNumber of entries: {zmap_uri_res.shape[0]}")

            # ----------- remove-duplicates -----------     => there could be a problem when dealing with a lot of data (multiple chunks) + multiple probes                                    
            print('-' * 100)
            print("\t DUPLICATES REMOVAL")
            time.sleep(MENU_WAIT)
            # clean the chunk by removing eventual duplicates -> 'probes' option
            zmap_uri_res = remove_duplicates(zmap_uri_res)
            print(f"\t\tNumber of unique entries: {zmap_uri_res.shape[0]}")

            # ----------- enrich ZMap result -----------
            zmap_uri_res['uri'] = uri
            # observability
            zmap_uri_res['observable'] = None

            for index, ip in enumerate(zmap_uri_res['saddr']):

                for data_pair in resources_and_ips[uri]:
                    
                    ip_addr = data_pair[0]
                    obs = data_pair[1]

                    if ip_addr == ip:
                        zmap_uri_res.loc[index, 'observable'] = obs
                        break
            
            # ----------- decode-zmap-payload -----------
            print('-' * 100)
            print("\t ZMAP BINARY DECODE")
            time.sleep(MENU_WAIT)
            # decode the ZMap results
            decode_res = asyncio.run(decode(zmap_uri_res))
            get_resources_df = decode_res[0]
            print(f"\n\t\t{decode_res[1]}")
            
            # debug
            print(f"{get_resources_df[['data', 'observable']]}")


            # ----------- store-get-dataframe -----------
            print('-' * 100)
            print("\t GET DATASET STORAGE")
            time.sleep(MENU_WAIT)
            # stored the cleaned chunk version in append mode
            payload_handling.options_to_json(get_resources_df).to_csv(output_paths[2]+ f"{datetime.date.today()}.csv", index=False, header=False, mode='a')
            print("\t\tCleaned version stored correctly!")
                
            # ----------- observe -----------
            print('-' * 100)
            print("\t OBSERVE RESOURCES")
            time.sleep(MENU_WAIT)
            # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
            observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]

            if observable_resources_df.empty:
                print("\n\t\tThere were NOT observable resources within the collected dataset")
            else:
                print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'data', 'data_length']]}")
                # store essential data
                observable_resources_df[['saddr', 'uri', 'data', 'data_length']].to_csv(output_paths[3] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')  

    return

################################################################################################

def zmap(cidr, cidr_id):

    # user can specify some custom ZMap command options
    #
    #               stability       new
    # bandwidth         x            x
    # probes            x            x
    # max-results                    x
    # 

    user_options = ['bandwidth', 'probes', 'max-results']
    zmap_user_cmd = []

    print("\n-----ZMap-Command-Specifications-----")

    for option in user_options:
        print(f"\tSpecify {option}:", end="")

        user_choice = input()

        if not user_choice: # if the user does not provide anything -> the option won't be added to the ZMap command
            continue
        else:
            zmap_user_cmd.append(f"--{option}={user_choice}")

    lookups(cidr, cidr_id, zmap_user_cmd)

    return

################################################################################################

def zmap_menu():

    # Header + Description
    print('-' * 50, '[ZMAP]', '-' * 50)
    print("Welcome to the ZMAP utility!")

    # NB: 224.0.0.0/3 -> not considered because of the blocklist
    # In my idea one partition for each VM
    internet_portions = ['0.0.0.0/3', '32.0.0.0/3', '64.0.0.0/3', '96.0.0.0/3', '128.0.0.0/3', '160.0.0.0/3', '192.0.0.0/3']

    # base path
    path = 'O1b_DataCollection/discovery/csv'

    print("From which portion of the Internet do you want to start?\n")

    # header
    print("\tindex".ljust(10), "cidr".ljust(19), "daily test".ljust(18))
    # options
    for index, portion in enumerate(internet_portions):
        
        # list of directories available at specified path -> list of DATES
        portion_dates = os.listdir(f"{path}/{index}")
        # getting today date
        today = str(datetime.date.today())

        # build up the info line to be printed
        info_line = "\t"
        info_line += f"{index}".ljust(10)       # add the index
        info_line += f"{portion}".ljust(20)     # add the portion cidr

        # helper feature to rememeber which portions have been already processed
        if today in portion_dates:
            info_line += "[Done]".ljust(20)     # if there's a 'today' directory -> already done
        else:
            info_line += "[To Do]".ljust(20)    # if there's NOT a 'today' directory -> to be done
    
        print(info_line)
    
    # user selects the portion id
    print("\nPlease select the index: ", end="")

    try:
        # get the index and convert it to integer
        internet_portion_id = int(input())
        # retrieve the cidr related
        internet_portion = internet_portions[internet_portion_id]
        # call the zmap() function
        zmap(internet_portion, internet_portion_id)

    except Exception as e:
        # print the error type
        print(e)
        # call a new instance of zmap_menu() function
        zmap_menu()


    return