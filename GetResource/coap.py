from aiocoap import *
import csv
import logging

from utils import files_handling
from utils import payload_handling

# log info settings
logging.basicConfig(level=logging.INFO)


# perform a CoAP GET request of a specified CoAP resource (IP addr + Resource URI)
async def get(ip_address, uri):

    # client context creation
    protocol = await Context.create_client_context()
    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{uri}"
    # build the request message
    request = Message(code=GET, uri=uri_to_check)

    try:
        # send the request and obtained the response
        response = await protocol.request(request).response

    except Exception as e:
        print(f"\t{e}")
        return None # error

    else:
        return response # success



# coroutine function: entry point of event loop
async def get_requests(partition_path):
    
    try:
        # OUTPUT FILE: GET results file 
        get_test_path = files_handling.new_gets_test(partition_path)
        
        # INPUT FILE: 
        # 1. coap partition size
        with open(files_handling.path_dict_to_str(partition_path), newline='') as coap_file:
            partition_size = sum(1 for _ in csv.reader(coap_file))

        # 2. coap partition csv
        with open(files_handling.path_dict_to_str(partition_path), newline='') as coap_file:

            partition_csv = csv.reader(coap_file)

            for row in partition_csv:
                
                if (partition_csv.line_num == 1):
                    continue

                print("ç" * 100)

                print(f"[{partition_csv.line_num-1}/{partition_size-1}] Testing: {row[0]}") # row[0] => ip address
                
                if (row[8] == False):
                    print(f"\tServer at {row[0]} was not active")
                    continue

                if (row[9] != '2.05 Content'):
                    print(f"\tEmpty/Invalid Resource Discovery Response at {row[0]}")
                    continue

                uris = payload_handling.uri_list_of(row[11]) # discovery payload = list of uris + their metadata

                for i, uri in enumerate(uris):

                    print(f"\t[{i+1}/{len(uris)}] GET request to: {uri}")

                    '''
                    await <COROUTIN_OBJ> 
                    -> await the discovery coroutine, pausing the execution of main until discovery completes
                    '''
                    # resource discovery response
                    response = await get(str(row[0]), uri[1:-1])

                    # debug
                    print(f"\t\tResponse: {response}")
                        
                    if response != None: # valid responses

                        if isinstance(response.payload, bytes):
                            try:
                                payload = response.payload.decode("utf-8")
                            except UnicodeDecodeError as e:
                                print(f"Failed to decode payload: {e}")
                                payload = response.payload  # fallback to raw bytes
                        else:
                            payload = response.payload  # already a str
                        
                        data_to_store = [row[0], # IP
                                         row[2], # as_name
                                         row[5], # country
                                         row[7], # continent
                                         uri,
                                         response.code,
                                         response.mtype,
                                         payload,
                                         len(response.payload)
                        ]

                        print(f"\t\t\tResponse Code: {response.code}")
                        print(f"\t\t\tMessage Type: {response.mtype}")
                        print(f"\t\t\tPayload: {payload}")
                        print(f"\t\t\tPayload Size: {len(response.payload)}")

                        files_handling.store_data(data_to_store, files_handling.path_dict_to_str(get_test_path))
                
        
        # 3. logs info about current get requests
        exp_name = partition_path['experiment']
        date = partition_path['date']
        part_id = partition_path['partition'].split('.')[0].split('_')[0]
        dataset_name = partition_path['dataset']

        log_info = [exp_name, date, part_id, dataset_name]

        files_handling.store_data(log_info, "utils/logs/get_checks.csv")
        


    except Exception as e:
        print(f"GET-requests/coap.py: {e}")