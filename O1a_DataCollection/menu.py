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
from O1a_DataCollection.coap import get_requests

################################################################################################

# constants definition

MENU_WAIT = 3
CHUNK_SIZE = 10000

################################################################################################

# perform the resource discovery operation -> GET (/.well-known/core)
async def discovery(ip_address):

    decoded_msg = {
        'version': None,
        'mtype': None,
        'token-length': None,
        'code': None,
        'mid': None,
        'token': None,
        'options': None,
        'data': None
    }

    print("\t\t\tPerforming new GET request ...")

    # client context creation
    protocol = await Context.create_client_context()
    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683/.well-known/core"
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
            'data': payload_handling.get_payload(response)
        })

        return decoded_msg # success

################################################################################################
# decode_payload()
#   I: binary payload (in hex)
#   O: structured object with multiple fields
def decode_payload(binary_payload):

    # default message to be returned (if any error occurs during the decoding process)
    decoded_msg = {
        'version': None,
        'mtype': None,
        'token-length': None,
        'code': None,
        'mid': None,
        'token': None,
        'options': None,
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
        'data': payload_handling.get_payload(msg)
    })

    return decoded_msg

################################################################################################

# decode()
#   it takes as input the raw/binary ZMap data field and it returns a structured object representing a CoAP response message
#   O: version, message type (mtype), token length, code (response code), mid (message id), toekn, options, data (payload)
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
            'data': None
        }

        # if success field is equal to 1 (all UDP kind of result)
        if row['success'] == 1:

            # decoding binary data and get a dictionary as result
            decoded_msg = decode_payload(row['data'])

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
                    decoded_msg = await discovery(row['saddr'])

                    # print the complete/not truncated data/payload field
                    print(f"ZMap Complete Payload decoded: {decoded_msg['data']}")

        # if success field is equal to 0 (all icmp, ... kind of result)
        else:
            decode_results.update([f"unsuccess/{row['icmp_unreach_str']}"])

        # each case (un/success) -> append and store it
        new_data_list.append(decoded_msg)
        print('£' * 50)

    # Build dataframe from list
    columns = ['version', 'mtype', 'token-length', 'code', 'mid', 'token', 'options', 'data']
    for col in columns:
        df_zmap[col] = [d[col] for d in new_data_list]

    # return datafram structure + summary
    return [df_zmap, decode_results]


################################################################################################

# remove_duplicates()
#   it is able to identify duplicates in the ZMap scan csv files: keep the first, discard the others
#   duplicate = both IP address and data fields equal
def remove_duplicates(df_zmap):

    # [subset=['saddr', 'data']] Remove duplicates considering 'saddr' and 'data' fields
    # [keep='first'] Keep the first instance (not delete all duplicates)
    # [inplace=True] Whether to modify the DataFrame rather than creating a new one.
    df_zmap.drop_duplicates(subset=['saddr', 'data'], keep='first', inplace=True)

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
        f'O1a_DataCollection/discovery/csv/{cidr_id}/{datetime.date.today()}/',
        # 2) Cleaned/Decoded csv file
        f'O1a_DataCollection/discovery/cleaned/{cidr_id}/{datetime.date.today()}/',
        # 3) Ip List csv file
        f'O1a_DataCollection/discovery/ip_lists/{cidr_id}/',
        # 4) Ip Info csv file
        f'O1a_DataCollection/discovery/ip_info/{cidr_id}/',
        # 5) Get Resources
        f'O1a_DataCollection/get/{cidr_id}/{datetime.date.today()}/',
        # 6) Observe Resources
        f'O1a_DataCollection/observe/{cidr_id}/{datetime.date.today()}/'
    ]                   
    
    # create a new empty csv file in all the previously elencated directories
    for file_path in output_paths:

        if not os.path.exists(file_path):
            os.makedirs(file_path)

        file_path += f"{datetime.date.today()}.csv"

        with open(file_path, 'w') as new_csv:
            pass

    #################################################

    # ZMap command definition
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
            # perform the GET requests to found ZMap resources
            get_resources_df = asyncio.run(get_requests(chunk[['saddr','code','success','data','options']]))
            payload_handling.options_to_json(get_resources_df).to_csv(output_paths[4] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')


            # ----------- observe -----------
            print('-' * 100)
            print("\t8. OBSERVE RESOURCES")
            time.sleep(MENU_WAIT)
            # consider only those entries having the observable field equal to 0 or 1 -> REAL OBS resources
            observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]
            print(f"\t\tObservable Resources: \n{observable_resources_df[['saddr', 'uri', 'payload', 'payload-length']]}")

            if observable_resources_df.empty:
                print("\n\t\tThere were NOT observable resources within the collected dataset")
            else:
                # store essential data
                observable_resources_df[['saddr', 'uri', 'payload', 'payload-length']].to_csv(output_paths[5] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')

            add_header = False

    return


################################################################################################

'''STABILITY LOOKUPS'''
def stability_lookups(cidr_id, zmap_user_cmd):

    # FIRST CHECK: are there any past dates?
    base_path = f'O1a_DataCollection/discovery/csv/{cidr_id}/'

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
        "--output-filter=\"repeat=0\"", # in case option probes > 1
        "-q"
    ]

    # enrich the base ZMap command with the user specified options
    cmd.extend(zmap_user_cmd)
    
    ##################################################
    
    # get the IPs to check from a previously created csv file
    ip_list_path = f'O1a_DataCollection/discovery/ip_lists/{cidr_id}/'

    ##################################################

    # must update all dates seen so far
    for date in dates:

        '''1) FILES CREATION'''
        # CSV output files creation
        output_paths = [
            # 1) Raw ZMap csv file
            f'O1a_DataCollection/discovery/csv/{cidr_id}/{date}/',
            # 2) Cleaned/Decoded csv file
            f'O1a_DataCollection/discovery/cleaned/{cidr_id}/{date}/',
            # 3) Get Resources
            f'O1a_DataCollection/get/{cidr_id}/{date}/',
            # 4) Observe Resources
            f'O1a_DataCollection/observe/{cidr_id}/{date}/'
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
                get_resources_df = asyncio.run(get_requests(chunk[['saddr','code','success','data','options']]))
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

    print()
    print('=' * 75)

    # STABILITY LOOKUPS
    # Investigate over already found CoAP servers -> test their stability over time
    print("\n1. [Stability Lookups] utility\n\tTest previously collected IP address to assess their stability")
    stability_lookups(cidr_id, zmap_user_cmd[:-1]) # NB: 'max-results' option is not allowed here

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
    path = 'O1a_DataCollection/discovery/csv'

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

    return