from aiocoap import *
import csv
import logging
import datetime

from utils.file_handling import files_handling

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
        coap_test_file = files_handling.new_coap_test(partition_path)
        
        # INPUT FILE: 
        # 1. zmap partition size
        with open(partition_path, newline='') as zmap_file:
            zmap_partition_size = sum(1 for _ in csv.reader(zmap_file))

        # 2. zmap partition csv
        with open(partition_path, newline='') as zmap_file:
            zmap_partition_csv = csv.reader(zmap_file)

            for row in zmap_partition_csv:

                print(f"[{zmap_partition_csv.line_num}/{zmap_partition_size}] Testing: {row[0]}") # row[0] => ip address

                '''
                await <COROUTIN_OBJ> 
                -> await the discovery coroutine, pausing the execution of main until discovery completes
                '''

                # resource discovery response
                response = await discovery(str(row[0]))

                # debug
                print(f"\tResponse: {response}")

                data_to_store = [row[0]] # storing IP address
                    
                if response != None: # VALID IPs
                    print(f"\t\t{row[0]} is a machine exposing a CoAP service")

                    # Store resource discovery data
                    data_to_store.extend([True, response.code, response.mtype, response.payload, len(response.payload)])

                else: # INVALID IPs
                    print(f"\t\t{row[0]} is NOT a machine exposing a CoAP service")
                        
                    # Store also invalid IPs
                    data_to_store.extend([False, None, None, None, None, None, None, None, None, None, None, None])
                
                print("-" * 100)

                files_handling.store_data(data_to_store, coap_test_file)
                
        
        # 3. logging operations
        path_fields = partition_path.split('/')
        
        exp_name = path_fields[3]
        date = datetime.date.today()
        n_part = path_fields[4].split('.')[0].split('_')[1]
        part_id = path_fields[4].split('.')[0].split('_')[0]
        dataset_name = path_fields[2]

        log_info = [exp_name, date, n_part, part_id, dataset_name]

        files_handling.store_data(log_info, "DataRefinement/logs/results_partitioned.csv")
        


                

    except Exception as e:
        print(f"coap.py: {e}")