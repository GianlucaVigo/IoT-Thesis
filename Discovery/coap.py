from aiocoap import *
import csv
import logging
import datetime

from utils import files_handling

# log info settings
logging.basicConfig(level=logging.INFO)


# perform the resource discovery operation -> GET (./well-known/core)
async def discovery(ip_address):

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
        return None # error

    else:
        return response # success



# coroutine function: entry point of event loop
async def findCoapServers(partition_path):
    
    try:
        # OUTPUT FILE: CoAP servers 
        discovery_test_path = files_handling.new_discovery_test(partition_path)
        
        # INPUT FILE: 
        # 1. ipinfo partition size
        with open(files_handling.path_dict_to_str(partition_path), newline='') as ipinfo_file:
            partition_size = sum(1 for _ in csv.reader(ipinfo_file))

        # 2. ipinfo partition csv
        with open(files_handling.path_dict_to_str(partition_path), newline='') as ipinfo_file:

            partition_csv = csv.reader(ipinfo_file)

            for row in partition_csv:

                print("^" * 75)

                print(f"[{partition_csv.line_num}/{partition_size}] Testing: {row[0]}") # row[0] => ip address

                '''
                await <COROUTIN_OBJ> 
                -> await the discovery coroutine, pausing the execution of main until discovery completes
                '''

                # resource discovery response
                response = await discovery(str(row[0]))

                # debug
                print(f"\tResponse: {response}")
                    
                if response != None: # VALID IPs
                    print(f"\t\t{row[0]} is a machine exposing a CoAP service")

                    # Store resource discovery data
                    row.extend([True, response.code, response.mtype, response.payload.decode("utf-8"), len(response.payload)])

                else: # INVALID IPs
                    print(f"\t\t{row[0]} is NOT a machine exposing a CoAP service")
                        
                    # Store also invalid IPs
                    row.extend([False, None, None, None, None])

                files_handling.store_data(row, files_handling.path_dict_to_str(discovery_test_path))
                
        
        # 3. logs info about current resource discovery
        exp_name = partition_path['experiment']
        date = str(datetime.date.today())
        part_id = partition_path['partition'][:1]
        dataset_name = partition_path['dataset']

        log_info = [exp_name, date, part_id, dataset_name]

        files_handling.store_data(log_info, "utils/logs/discovery_checks.csv")
        


    except Exception as e:
        print(f"Discovery/coap.py: {e}")