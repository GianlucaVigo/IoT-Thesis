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

MENU_WAIT = 3
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

# perform the resource discovery operation -> GET (/.well-known/core)
async def get(ip_address, res_uri):

    decoded_msg = {
        'version': None,
        'mtype': None,
        'token-length': None,
        'code': None,
        'mid': None,
        'token': None,
        'options': None,
        'observable': None,
        'data': None
    }

    print(f"\t\t\tPerforming new GET request to {res_uri} ...")

    # client context creation
    protocol = await Context.create_client_context()
    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{res_uri}"
    # build the request message
    request = Message(code=GET, uri=uri_to_check)


    try:
        # send the request and obtained the response
        response = await protocol.request(request).response

    except Exception as e:
        print(f"\t{e}")
        return decoded_msg

    else:

        decoded_msg.update({
            'version': payload_handling.get_version(response),
            'mtype': payload_handling.get_mtype(response),
            'token-length': payload_handling.get_token_length(response),
            'code': payload_handling.get_code(response),
            'mid': payload_handling.get_mid(response),
            'token': payload_handling.get_token(response),
            'options': payload_handling.get_options(response),
            'observable': payload_handling.get_observe(response, res_uri),
            'data': payload_handling.get_payload(response)
        })

        return decoded_msg # success

################################################################################################
# decode_payload()
#   I: binary payload (in hex)
#   O: structured object with multiple fields
def decode_payload(binary_payload, uri):

    # default message to be returned (if any error occurs during the decoding process)
    decoded_msg = {
        'version': None,
        'mtype': None,
        'token-length': None,
        'code': None,
        'mid': None,
        'token': None,
        'options': None,
        'observable': None,
        'data': binary_payload     # maintain the raw payload as is
    }

    # ----- decoding ZMap binary message -----

    try:

        msg = Message.decode(bytes.fromhex(binary_payload))

    # unable to decode the message
    except Exception as e:
        print(f'Message decoding error: {e}')
        return decoded_msg
    
    # ----------------------------------------

    # update/populate the starting decoded message structure with all retrieved info/data
    decoded_msg.update({
        'version': payload_handling.get_version(msg),
        'mtype': payload_handling.get_mtype(msg),
        'token-length': payload_handling.get_token_length(msg),
        'code': payload_handling.get_code(msg),
        'mid': payload_handling.get_mid(msg),
        'token': payload_handling.get_token(msg),
        'options': payload_handling.get_options(msg),
        'observable': payload_handling.get_observe(msg, uri),
        'data': payload_handling.get_payload(msg)
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

    # iterate over rows
    for _, row in df_zmap.iterrows():

        # default decoded message (it will be returned if any error occurs)
        decoded_msg = {
            'version': None,
            'mtype': None,
            'token-length': None,
            'code': None,
            'mid': None,
            'token': None,
            'options': None,
            'observable': None,
            'data': None
        }

        # if success field is equal to 1 (all UDP kind of result)
        if row['success'] == 1:

            # decoding binary data and get a dictionary as result
            decoded_msg = decode_payload(row['data'], row['uri'])

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
                    
                    # -> if so, perform a new discovery through aiocoap library
                    decoded_msg = await get(row['saddr'], row['uri'])

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

    # Build dataframe from list
    columns = ['version', 'mtype', 'token-length', 'code', 'mid', 'token', 'options', 'data', 'observable']
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

'''NEW INTERNET WIDE SEARCH'''
def new_internet_wide_search(cidr, cidr_id, zmap_user_cmd):

    '''1) FILES CREATION'''
    # CSV output files creation
    output_paths = [
        # 1) Raw ZMap csv file
        f'O1b_DataCollection/discovery/csv/{cidr_id}/{datetime.date.today()}/',
        # 2) Cleaned/Decoded csv file         
        f'O1b_DataCollection/discovery/cleaned/{cidr_id}/{datetime.date.today()}/',
        # 3) Ip List csv file     
        f'O1b_DataCollection/discovery/ip_lists/{cidr_id}/',
        # 4) Ip Info csv file                            
        f'O1b_DataCollection/discovery/ip_info/{cidr_id}/',
        # 5) Get Resources
        f'O1b_DataCollection/get/{cidr_id}/{datetime.date.today()}/',
        # 6) Observe
        f'O1b_DataCollection/observe/{cidr_id}/{datetime.date.today()}/'                               
    ] 
    
    # create a new empty csv file in all the previously elencated directories
    for file_path in output_paths:

        if not os.path.exists(file_path):
            os.makedirs(file_path)

        file_path += f"{datetime.date.today()}.csv"

        with open(file_path, 'w') as new_csv:
            pass

    #################################################

    # DISCOVERY: ZMap command definition
    cmd = [
        "zmap",
        "--target-port=5683",
        "--probe-module=udp",
        "--blocklist-file=utils/zmap/conf/blocklist.conf",
        "--probe-args=file:utils/zmap/examples/udp-probes/coap_5683.pkt",
        "--output-module=csv",
        "--output-fields=*",
        "--output-file=" + output_paths[0] + f"{datetime.date.today()}.csv",
        "--output-filter=\"repeat=0\"",
        "-q"
    ]

    # enrich the base ZMap command with the user specified options
    cmd.extend(zmap_user_cmd)
    cmd.append(cidr)
    
    #################################################

    '''2) DISCOVERY ZMAP EXECUTION'''
    # ZMap Command Execution
    # stdout=subprocess.PIPE    => captures the process's std output so that Python can read it
    # stderr=subprocess.STDOUT  => redirect the std error to output and so to Python
    # text=True                 => convert the std output into text (instead of bytes)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


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

    #################################################

    resources_and_ips = {}

    '''3) ELABORATE DISCOVERY ZMAP RESULTS'''
    # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE    
    with pd.read_csv(output_paths[0] + f"{datetime.date.today()}.csv", chunksize=CHUNK_SIZE) as csv_reader:

        # when an header is necessary, it must be happended on the first chunk only
        add_header = True

        for i, chunk in enumerate(csv_reader):

            # ----------- chunk-info -----------
            print(f"\tChunk nr [{i+1}]")

            # ----------- raw-master-dataset -----------
            print('-' * 100)
            print("\t1. ZMAP RAW DATASET")
            # print number of entries in the current chunk
            print(f"\t\tNumber of entries: {chunk.shape[0]}")

            # ----------- remove-duplicates -----------     => there could be a problem when dealing with a lot of data (multiple chunks) + multiple probes                                    
            print('-' * 100)
            print("\t2. DUPLICATES REMOVAL")
            time.sleep(MENU_WAIT)
            # clean the chunk by removing eventual duplicates -> 'probes' option
            chunk = remove_duplicates(chunk)
            print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

            # ----------- decode-zmap-payload -----------
            print('-' * 100)
            print("\t3. ZMAP BINARY DECODE")
            time.sleep(MENU_WAIT)
            # decode the ZMap results
            chunk['uri'] = '/.well-known/core'
            chunk['observable'] = None
            decode_res = asyncio.run(decode(chunk))
            chunk = decode_res[0]
            print(f"\n\t\t{decode_res[1]}")

            # ----------- store-discovery-dataframe -----------
            print('-' * 100)
            print("\t4. DISCOVERY DATASET STORAGE")
            time.sleep(MENU_WAIT)
            # stored the cleaned chunk version in append mode
            payload_handling.options_to_json(chunk).to_csv(output_paths[1] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')
            print("\t\tCleaned version stored correctly!")

            # ----------- ip-list -----------
            print('-' * 100)
            print("\t5. EXTRACTING IP LIST")
            time.sleep(MENU_WAIT)
            chunk[['saddr']].to_csv(output_paths[2] + f"{datetime.date.today()}.csv", index=False, header=False, mode='a')

            # ----------- ip-info -----------
            print('-' * 100)
            print("\t6. ADDITIONAL IP INFORMATION EXTRACTION")
            time.sleep(MENU_WAIT)
            # extract and store the IP addresses collected by ZMap processing
            ip_info_df = extract_ip_info(chunk[['saddr']])
            ip_info_df.to_csv(output_paths[3] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')

            # ----------- get-resources -----------
            print('-' * 100)
            print("\t7. GET RESOURCES")
            time.sleep(MENU_WAIT)

            for _,row in chunk[['saddr','code','success','data','options']].iterrows():

                #########################################
                # When can GET requests be avoided?

                # ----- SUCCESS check -----
                if row['success'] == 0: # success = 0/1
                    print(f"\t\tNot successful response to resource discovery: \n\t\tskip {row['saddr']}")
                    continue

                # ----- CODE check -----
                if row['code'] != '2.05 Content': # not successful discovery (= 2.05 Content)
                    print(f"\t\tNot '2.05 Content' discovery: \n\t\tskip {row['saddr']}")
                    continue

                # ----- OPTION check -----
                # Assumption: /.well-known/core resource use the CONTENT FORMAT option equal to LINK FORMAT
                # NB: I filter out everything that is not in LINKFORMAT style (TEXT, ...)
                if row['options'] != None:

                    options_dict = eval(row['options'])

                    if 'CONTENT_FORMAT' in options_dict.keys():
                        if options_dict['CONTENT_FORMAT'] != 'LINKFORMAT':
                            continue

                # ----- EMPTY PAYLOAD check -----
                if len(row['data']) == 0: # empty payload string
                    print(f"\t\tEmpty payload was returned from resource discovery: \n\t\tskip {row['saddr']}")
                    continue

                # ----- STILL BINARY PAYLOAD check -----
                if isinstance(row['data'], bytes):
                    print(f"\t\tCan't decode its binary payload: \n\t\tskip {row['saddr']}")
                    continue
                #########################################

                # resources = list of uris + metadata
                resources = payload_handling.resource_list_of(row['data'])

                for res in resources:

                    uri = res.split(';')[0]

                    cleaned_uri = uri.strip('<>')
                    
                    # --------- uris-validity-check ---------
                    # 1. uri must start with '/'
                    if not cleaned_uri.startswith('/'):
                        continue

                    ip = row['saddr']
                    obs = payload_handling.get_metadata_value_of(res, 'obs')

                    if cleaned_uri in resources_and_ips.keys():
                        resources_and_ips[cleaned_uri].append([ip,obs])
                    else:
                        resources_and_ips[cleaned_uri] = [[ip,obs]]

    # supporting files
    ips_support_file = "O1b_DataCollection/get/ips.csv"
    res_support_file = "O1b_DataCollection/get/res.csv"

    # header
    with open(output_paths[4] + f"{datetime.date.today()}.csv", 'w') as new_csv:
        new_csv.write("saddr,saddr_raw,daddr,daddr_raw,ipid,ttl,classification,success,sport,dport,udp_pkt_size,data,icmp_responder,icmp_type,icmp_code,icmp_unreach_str,repeat,cooldown,timestamp_str,timestamp_ts,timestamp_us,uri,version,mtype,token-length,code,mid,token,options,observable\n")

    # consider one uri at a time
    for i, uri in enumerate(resources_and_ips.keys()):

        # write the list of IPs into a support file, then used by ZMap
        rows = []
        
        for item in resources_and_ips[uri]:
            rows.append(item[0]) # append IP

        ips_df = pd.DataFrame(rows)
        ips_df.to_csv(ips_support_file, index=False, header=False)

        # probe hex encoding: incorporate the uri
        probe_encoded = encode_uri(uri)

        print('#' * 75)
        print(f"{i+1}/{len(resources_and_ips.keys())}")
        print(f"\tUri: {uri}")
        print(f"\tNum of IPs: {len(resources_and_ips[uri])}")
        print(f"\tProbe: {probe_encoded}")


        # DISCOVERY: ZMap command definition
        cmd = [
            "zmap",
            "--target-port=5683",
            "--probe-module=udp",
            "--blocklist-file=utils/zmap/conf/blocklist.conf",
            "--allowlist-file=" + ips_support_file,
            "--probe-args=hex:" + probe_encoded,
            "--output-module=csv",
            "--output-fields=*",
            "--output-file=" + res_support_file,
            "--cooldown=21",
            "--probes=3",
            "--output-filter=\"repeat=0\""
        ]
        
        #################################################

        '''2) DISCOVERY ZMAP EXECUTION'''
        # ZMap Command Execution
        # stdout=subprocess.PIPE    => captures the process's std output so that Python can read it
        # stderr=subprocess.STDOUT  => redirect the std error to output and so to Python
        # text=True                 => convert the std output into text (instead of bytes)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


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

        #################################################

        zmap_uri_res = pd.read_csv(res_support_file)

        zmap_uri_res = remove_duplicates(zmap_uri_res)

        # include uri in the results to be stored
        zmap_uri_res['uri'] = uri
        zmap_uri_res['observable'] = False
        zmap_uri_res['data_length'] = zmap_uri_res['data'].apply(len)

        # observability
        for index, ip in enumerate(zmap_uri_res['saddr']):

            for data_pair in resources_and_ips[uri]:
                
                ip_addr = data_pair[0]
                obs = data_pair[1]

                if ip_addr == ip:
                    if obs == True:
                        zmap_uri_res.loc[index, 'observable'] = obs
                        break

        # print the data column
        print(zmap_uri_res[['data', 'observable']])

        # decode the ZMap get results
        decode_res = asyncio.run(decode(zmap_uri_res))

        print(decode_res[0]['observable'])

        print(decode_res[1])

        get_resources_df = decode_res[0]
        

        # store them
        payload_handling.options_to_json(decode_res[0]).to_csv(output_paths[4]+ f"{datetime.date.today()}.csv", index=False, header=False, mode='a')

        # timer needed to close/end correctly the ZMap process
        time.sleep(3)

        
        # ----------- observe -----------
        print('-' * 100)
        print("\t8. OBSERVE RESOURCES")
        time.sleep(MENU_WAIT)
        # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
        observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]
        print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'data', 'data_length', 'observable']]}")

        if observable_resources_df.empty:
            print("\n\t\tThere were NOT observable resources within the collected dataset")
        else:
            # store essential data
            observable_resources_df[['saddr', 'uri', 'data', 'data_length', 'observable']].to_csv(output_paths[5] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')
        

        add_header = False

    return

################################################################################################

'''STABILITY LOOKUPS'''
def stability_lookups(cidr_id, zmap_user_cmd):

    # FIRST CHECK: are there any past dates?
    base_path = f'O1_AllZMap/scan_results/csv/{cidr_id}/'

    # list of directories related to eventual dates already analysed
    dates = os.listdir(base_path)

    # if no previous dates -> first time of execution of the program -> return and get NEW/FRESH data
    if len(dates) == 0:
        print("\t\tNo data recorded yet!")
        return

    ##################################################

    # ZMap command definition
    cmd = [
        "zmap",
        "--target-port=5683",
        "--probe-module=udp",
        "--blocklist-file=utils/zmap/conf/blocklist.conf",
        "--probe-args=file:utils/zmap/examples/udp-probes/coap_5683.pkt",
        "--output-module=csv",
        "--output-fields=*",
        "-q"
    ]

    # enrich the base ZMap command with the user specified options
    cmd.extend(zmap_user_cmd)
    
    ##################################################
    
    # get the IPs to check from a previously created csv file
    ip_list_path = f'O1_AllZMap/scan_results/ip_lists/{cidr_id}/'

    ##################################################

    # must update all dates seen so far
    for date in dates:

        '''1) FILES CREATION'''
        # CSV output files creation
        output_paths = [
            f'O1_AllZMap/scan_results/csv/{cidr_id}/{date}/',              # 1) Raw ZMap csv file
            f'O1_AllZMap/scan_results/cleaned/{cidr_id}/{date}/'           # 2) Cleaned/Decoded csv file
        ]
        
        # create a new empty csv file in all the previously elencated directories
        for file_path in output_paths:

            if not os.path.exists(file_path):
                os.makedirs(file_path)

            file_path += f"{datetime.date.today()}.csv"

            # create the empty file
            with open(file_path, 'w') as new_csv:
                pass

        #################################################

        # Update ZMap commad
        cmd.append("--output-file=" + output_paths[0] + f"{datetime.date.today()}.csv")
        cmd.append("--allowlist-file=" + ip_list_path + f"{date}.csv")
    
        #################################################

        '''2) ZMAP EXECUTION'''
        # ZMap Command Execution
        # stdout=subprocess.PIPE    => captures the process's std output so that Python can read it
        # stderr=subprocess.STDOUT  => redirect the std error to output and so to Python
        # text=True                 => convert the std output into text (instead of bytes)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

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

        #################################################

        '''3) ELABORATE ZMAP RESULTS'''
        # since the ZMap result could be large, I split the output csv file into chunks having size CHUNK_SIZE
        with pd.read_csv(output_paths[0] + f"{datetime.date.today()}.csv", chunksize=CHUNK_SIZE) as csv_reader:

            # when an header is necessary, it must be happended on the first chunk only
            add_header = True

            for i, chunk in enumerate(csv_reader):

                # ----------- chunk-info -----------
                print(f"\tChunk nr [{i+1}]")

                # ----------- raw-master-dataset -----------
                print('-' * 100)
                print("\t1. ZMAP RAW DATASET")
                # print number of entries in the current chunk
                print(f"\t\tNumber of entries: {chunk.shape[0]}")   

                # ----------- remove-duplicates -----------     => there could be a problem when dealing with a lot of data (multiple chunks)
                #                                                   + multiple probes
                print('-' * 100)
                print("\t2. DUPLICATES REMOVAL")
                time.sleep(MENU_WAIT)
                # clean the chunk by removing eventual duplicates -> 'probes' option
                chunk = remove_duplicates(chunk)
                print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

                # ----------- decode-zmap-payload -----------
                print('-' * 100)
                print("\t3. ZMAP BINARY DECODE")
                time.sleep(MENU_WAIT)
                # decode the ZMap results
                decode_res = asyncio.run(decode(chunk))
                chunk = decode_res[0]
                print(f"\n\t\t{decode_res[1]}")

                # ----------- store-discovery-dataframe -----------
                print('-' * 100)
                print("\t4. DISCOVERY DATASET STORAGE")
                time.sleep(MENU_WAIT)
                # stored the cleaned chunk version in append mode
                payload_handling.options_to_json(chunk).to_csv(output_paths[1] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')
                print("\t\tCleaned version stored correctly!")

                # ----------- get-resources -----------
                print('-' * 100)
                print("\t5. GET RESOURCES")
                time.sleep(MENU_WAIT)
                # perform the GET requests to found ZMap resources
                get_resources_df = asyncio.run(get_requests(chunk[['saddr','code','success','data']]))
                payload_handling.options_to_json(get_resources_df).to_csv(output_paths[2] + f"{datetime.date.today()}.csv",  index=False, header=add_header, mode='a')

                # ----------- observe -----------
                print('-' * 100)
                print("\t6. OBSERVE RESOURCES")
                time.sleep(MENU_WAIT)
                # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
                observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]
                print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'payload', 'payload-length']]}")

                if observable_resources_df.empty:
                    print("\n\t\tThere were NOT observable resources within the collected dataset")
                else:
                    # store essential data
                    observable_resources_df[['saddr', 'uri', 'payload', 'payload-length']].to_csv(output_paths[3] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')

                add_header = False
                
        #################################################

        # Update ZMap commad
        cmd.pop() # output-file specs
        cmd.pop() # allowlist-file specs
    
        #################################################
        
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

    print("-------------------------------------")

    #print()
    #print('=' * 75)

    # STABILITY LOOKUPS
    # Investigate over already found CoAP servers -> test their stability over time
    #print("\n1. [Stability Lookups] utility\n\tTest previously collected IP address to assess their stability")
    #stability_lookups(cidr_id, zmap_user_cmd[:-1]) # NB: 'max-results' option is not allowed here

    print()
    print('=' * 75)

    # NEW LOOKUP
    # Acquire new daily snapshots
    print("\n2. [New Internet Wide Lookup] utility\n\tPerform a complete new Internet discovery related to the portion selected -> FRESH/NEW DATA")
    new_internet_wide_search(cidr, cidr_id, zmap_user_cmd)

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