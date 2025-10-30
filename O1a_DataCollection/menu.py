import asyncio
import pandas as pd
import time
import datetime

from aiocoap import *
from collections import Counter

from utils import payload_handling, workflow_handling, zmap_handling
from O1a_DataCollection.coap import get_requests

################################################################################################

# constants definition

MENU_WAIT = 0.5
CHUNK_SIZE = 10000

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

'''NEW INTERNET WIDE SEARCH'''
def lookups(cidr, cidr_id, zmap_user_cmd):

    '''1) FILES CREATION'''
    # CSV output files creation
    output_paths = [
        # 1) Raw ZMap csv file
        f'O1a_DataCollection/discovery/csv/{cidr_id}/{datetime.date.today()}/',
        # 2) Cleaned/Decoded csv file
        f'O1a_DataCollection/discovery/cleaned/{cidr_id}/{datetime.date.today()}/',
        # 4) Ip Info csv file
        f'O1a_DataCollection/discovery/ip_info/{cidr_id}/',
        # 5) Get Resources
        f'O1a_DataCollection/get/{cidr_id}/{datetime.date.today()}/',
        # 6) Observe Resources
        f'O1a_DataCollection/observe/{cidr_id}/{datetime.date.today()}/'
    ]                   


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