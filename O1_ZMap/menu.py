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
from O2_GetResource.coap import get_requests
from O3_Observe.coap import observe_resources

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

    print("\t\t\tPerforming new discovery ...")

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


def decode_payload(binary_payload):

    decoded_msg = {
        'version': None,
        'mtype': None,
        'token-length': None,
        'code': None,
        'mid': None,
        'token': None,
        'options': None,
        'data': binary_payload     # maintain the raw payload
    }

    # ----- decoding ZMap binary message -----

    try:

        msg = Message.decode(bytes.fromhex(binary_payload))

    # unable to decode the message
    except Exception as e:

        print(f'Message decoding error: {e}')
        return decoded_msg
    
    # ----------------------------------------

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


async def decode(df_zmap):

    new_data_list = []
    decode_results = Counter()

    for index, row in df_zmap.iterrows():

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

        #try:
        # if success field is equal to 1 (all udp kind of result)
        if row['success'] == 1:

                # decoding binary data and get a dictionary as result
                decoded_msg = decode_payload(row['data'])

                print(f"ZMap Payload decoded: {decoded_msg['data']}")

                # if code field is equal to None -> something went wrong
                if decoded_msg['code'] is None:
                    decode_results.update([f"unsuccess/{decoded_msg['data']}"])

                # otherwise it is a success
                else:
                    decode_results.update(['success'])

                    if payload_handling.detect_truncated_discovery(row['udp_pkt_size'], row['data'], decoded_msg):

                        decoded_msg = await discovery(row['saddr'])

                        print(f"ZMap Complete Payload decoded: {decoded_msg['data']}")

        # if success field is equal to 0 (all icmp, ... kind of result)
        else:
            decode_results.update([f"unsuccess/{row['icmp_unreach_str']}"])

        new_data_list.append(decoded_msg)
        print('£' * 50)

    # Build dataframe safely
    columns = ['version', 'mtype', 'token-length', 'code', 'mid', 'token', 'options', 'data']
    for col in columns:
        df_zmap[col] = [d[col] for d in new_data_list]

    return [df_zmap, decode_results]


################################################################################################

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

def extract_ip_info(ip_list_df):

    ip_info_df = pd.DataFrame()

    with maxminddb.open_database('utils/ipinfo/ipinfo_lite.mmdb') as reader:

        for index, row in ip_list_df.iterrows():
            info = reader.get(row['saddr'])
            info_df = pd.DataFrame.from_dict([info])
            ip_info_df = pd.concat([ip_info_df, info_df], ignore_index=True)

        reader.close()
        
    # merge original IPs with retrieved info
    ip_info_df = pd.concat([ip_list_df.reset_index(drop=True), ip_info_df], axis=1)

    return ip_info_df

################################################################################################

'''NEW INTERNET WIDE SEARCH'''
def new_internet_wide_search(cidr, cidr_id, zmap_user_cmd):

    # CSV output files creation
    output_paths = [f'O1_ZMap/scan_results/csv/{cidr_id}/{datetime.date.today()}/',         # 1) Raw ZMap csv file
                    f'O1_ZMap/scan_results/cleaned/{cidr_id}/{datetime.date.today()}/',     # 2) Cleaned/Decoded csv file
                    f'O1_ZMap/scan_results/ip_lists/{cidr_id}/',                            # 3) Ip List csv file
                    f'O1_ZMap/scan_results/ip_info/{cidr_id}/',                             # 4) Ip Info csv file
                    f'O2_GetResource/csv/{cidr_id}/{datetime.date.today()}/',               # 5) Get Resources
                    f'O3_Observe/csv/{cidr_id}/{datetime.date.today()}/']                   # 6) Observe Resources
    
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
        "-q",
        "--output-file=" + output_paths[0] + f"{datetime.date.today()}.csv"
    ]

    cmd.extend(zmap_user_cmd)
    cmd.append(cidr)

    print(cmd)
    
    #################################################

    # ZMap Command Execution
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # Print lines as zmap runs (also useful for debug)
    try:
        while True:
            line = p.stdout.readline()
            if not line and p.poll() is not None:
                break
            if line:
                print("[zmap]", line.rstrip())
    finally:
        p.stdout.close()
        ret = p.wait()

    print("zmap exit:", ret)

    with pd.read_csv(output_paths[0] + f"{datetime.date.today()}.csv", chunksize=CHUNK_SIZE) as csv_reader:

        add_header = True

        for i, chunk in enumerate(csv_reader):

            # ----------- chunk-info -----------
            print(f"\tChunk nr [{i+1}]")

            # ----------- raw-master-dataset -----------
            print('-' * 100)
            print("\t1. ZMAP RAW DATASET")
            print(f"\t\tNumber of entries: {chunk.shape[0]}")

            # ----------- remove-duplicates -----------
            print('-' * 100)
            print("\t2. DUPLICATES REMOVAL")
            time.sleep(MENU_WAIT)
            chunk = remove_duplicates(chunk)
            print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

            # ----------- decode-zmap-payload -----------
            print('-' * 100)
            print("\t3. ZMAP BINARY DECODE")
            time.sleep(MENU_WAIT)
            decode_res = asyncio.run(decode(chunk))
            chunk = decode_res[0]
            print(f"\n\t\t{decode_res[1]}")

            # ----------- store-discovery-dataframe -----------
            # The filtered/cleaned version of the discovery dataset is going to be stored in the 'cleaned' folder
            print('-' * 100)
            print("\t4. DISCOVERY DATASET STORAGE")
            time.sleep(MENU_WAIT)
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
            ip_info_df = extract_ip_info(chunk[['saddr']])
            # The additional IP information is going to be stored in a proper file stored inside the 'ip_info' folder
            ip_info_df.to_csv(output_paths[3] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')


            # ----------- get-resources -----------
            print('-' * 100)
            print("\t7. GET RESOURCES")
            time.sleep(MENU_WAIT)
            get_resources_df = asyncio.run(get_requests(chunk[['saddr','code','success','data']]))
            payload_handling.options_to_json(get_resources_df).to_csv(output_paths[4] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')


            # ----------- observe -----------
            print('-' * 100)
            print("\t8. OBSERVE RESOURCES")
            time.sleep(MENU_WAIT)
            observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]
            print(f"\t\tObservable Resources: \n{observable_resources_df}")

            if observable_resources_df.empty:
                print("\n\t\tThere were NOT observable resources within the collected dataset")
            else:
                observable_resources_df.to_csv(output_paths[5] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')

            add_header = False

    return


################################################################################################

'''STABILITY LOOKUPS'''
def stability_lookups(cidr_id, zmap_user_cmd):

    # First check: are there any past dates?
    base_path = f'O1_ZMap/scan_results/csv/{cidr_id}/'

    dates = os.listdir(base_path)

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

    cmd.extend(zmap_user_cmd)

    print(cmd)
    
    ##################################################
    
    # Get the IPs to check from a previously created csv file
    ip_list_path = f'O1_ZMap/scan_results/ip_lists/{cidr_id}/'

    ##################################################

    # must update all dates seen so far
    for date in dates:

        # CSV output files creation
        output_paths = [f'O1_ZMap/scan_results/csv/{cidr_id}/{date}/',              # 1) Raw ZMap csv file
                        f'O1_ZMap/scan_results/cleaned/{cidr_id}/{date}/',          # 2) Cleaned/Decoded csv file
                        f'O2_GetResource/csv/{cidr_id}/{date}/',                    # 3) Get Resources
                        f'O3_Observe/csv/{cidr_id}/{date}/']                        # 4) Observe Resources
        
        for file_path in output_paths:

            if not os.path.exists(file_path):
                os.makedirs(file_path)

            file_path += f"{datetime.date.today()}.csv"

            with open(file_path, 'w') as new_csv:
                pass

        #################################################

        # Update ZMap commad
        cmd.append("--output-file=" + output_paths[0] + f"{datetime.date.today()}.csv")
        cmd.append("--allowlist-file=" + ip_list_path + f"{date}.csv")
    
        #################################################

        # ZMap Command Execution
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # Print lines as zmap runs (also useful for debug)
        try:
            while True:
                line = p.stdout.readline()
                if not line and p.poll() is not None:
                    break
                if line:
                    print("[zmap]", line.rstrip())
        finally:
            p.stdout.close()
            ret = p.wait()

        print("zmap exit:", ret)

        with pd.read_csv(output_paths[0] + f"{datetime.date.today()}.csv", chunksize=CHUNK_SIZE) as csv_reader:

            add_header = True

            for i, chunk in enumerate(csv_reader):

                # ----------- chunk-info -----------
                print(f"\tChunk nr [{i+1}]")

                # ----------- raw-master-dataset -----------
                print('-' * 100)
                print("\t1. ZMAP RAW DATASET")
                print(f"\t\tNumber of entries: {chunk.shape[0]}")

                # ----------- remove-duplicates -----------
                print('-' * 100)
                print("\t2. DUPLICATES REMOVAL")
                time.sleep(MENU_WAIT)
                chunk = remove_duplicates(chunk)
                print(f"\t\tNumber of unique entries: {chunk.shape[0]}")

                # ----------- decode-zmap-payload -----------
                print('-' * 100)
                print("\t3. ZMAP BINARY DECODE")
                time.sleep(MENU_WAIT)
                decode_res = asyncio.run(decode(chunk))
                chunk = decode_res[0]
                print(f"\n\t\t{decode_res[1]}")

                # ----------- store-discovery-dataframe -----------
                print('-' * 100)
                print("\t4. DISCOVERY DATASET STORAGE")
                time.sleep(MENU_WAIT)
                payload_handling.options_to_json(chunk).to_csv(output_paths[1] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')
                print("\t\tCleaned version stored correctly!")

                # ----------- get-resources -----------
                print('-' * 100)
                print("\t5. GET RESOURCES")
                time.sleep(MENU_WAIT)
                get_resources_df = asyncio.run(get_requests(chunk[['saddr','code','success','data']]))
                payload_handling.options_to_json(get_resources_df).to_csv(output_paths[2] + f"{datetime.date.today()}.csv",  index=False, header=add_header, mode='a')

                # ----------- observe -----------
                print('-' * 100)
                print("\t6. OBSERVE RESOURCES")
                time.sleep(MENU_WAIT)
                observable_resources_df = get_resources_df[(get_resources_df['observable'] == 0) | (get_resources_df['observable'] == 1)]
                print(f"\t\tObservable Resources: \n{observable_resources_df}")

                if observable_resources_df.empty:
                    print("\n\t\tThere were NOT observable resources within the collected dataset")
                else:
                    observable_resources_df.to_csv(output_paths[3] + f"{datetime.date.today()}.csv", index=False, header=add_header, mode='a')

                add_header = False
                
        #################################################

        # Update ZMap commad
        cmd.pop() # output-file specs
        cmd.pop() # allowlist-file specs
    
        #################################################
        
    return


################################################################################################

def zmap(cidr, cidr_id):

    user_options = ['bandwidth', 'probes', 'max-results']
    zmap_user_cmd = []

    print("\n-----ZMap-Command-Specifications-----")

    for option in user_options:
        print(f"\tSpecify {option}:", end="")

        user_choice = input()

        if not user_choice:
            continue
        else:
            zmap_user_cmd.append(f"--{option}={user_choice}")

    print("-------------------------------------")

    print()
    print('=' * 75)

    print("\n1. [Stability Lookups] utility\n\tTest previously collected IP address to assess their stability")
    stability_lookups(cidr_id, zmap_user_cmd[:-1])

    print()
    print('=' * 75)

    print("\n2. [New Internet Wide Lookup] utility\n\tPerform a complete new Internet discovery related to the portion selected -> FRESH/NEW DATA")
    new_internet_wide_search(cidr, cidr_id, zmap_user_cmd)

    return



def zmap_menu():

    # Header
    print('-' * 50, '[ZMAP]', '-' * 50)
    # Description
    print("Welcome to the ZMAP utility!")

    # NB: 224.0.0.0/3 -> not considered because of the blocklist
    internet_portions = ['0.0.0.0/3', '32.0.0.0/3', '64.0.0.0/3', '96.0.0.0/3', '128.0.0.0/3', '160.0.0.0/3', '192.0.0.0/3']

    path = 'O1_ZMap/scan_results/csv'

    print("From which portion of the Internet do you want to start?\n")

    # printing header
    print("\tindex".ljust(10), "cidr".ljust(19), "daily test".ljust(18))

    # printing options
    for index, portion in enumerate(internet_portions):

        portion_dates = os.listdir(f"{path}/{index}")
        today = str(datetime.date.today())

        info_line = "\t"
        info_line += f"{index}".ljust(10)
        info_line += f"{portion}".ljust(20)

        if today in portion_dates:
            info_line += "[Done]".ljust(20)
        else:
            info_line += "[To Do]".ljust(20) 
    
        print(info_line)
    

    print("\nPlease select the index: ", end="")
    internet_portion_id = int(input())

    zmap(internet_portions[internet_portion_id], internet_portion_id)

    return