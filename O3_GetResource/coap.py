from aiocoap import *
import csv
import logging
import os

from utils import files_handling
from utils import payload_handling

# log info settings
logging.basicConfig(level=logging.INFO)


def new_gets_test(partition_path):

    # BEFORE: 
    # 'phase': 'O2_ZMapDecoded', 
    # 'folder': 'csv',
    # 'dataset': '01_output',
    # 'experiment': 'exp_1', 
    # 'date': '2025-09-17',
    # 'partition': 0

    get_test_path = {
        'phase' : 'O5_GetResource',
        'folder': partition_path['folder'],
        'dataset': partition_path['dataset'],
        'experiment': partition_path['experiment'],
        'date': partition_path['date']
    }

    # AFTER:
    # 'phase': 'O5_GetResource', 
    # 'folder': 'csv',
    # 'dataset': '01_output',
    # 'experiment': 'exp_1', 
    # 'date': '2025-09-17'


    ######################################################


    get_test_path_str = files_handling.path_dict_to_str(get_test_path)
    
    # if directories do not exist -> create them
    if not os.path.exists(get_test_path_str):
        os.makedirs(get_test_path_str)


    ######################################################


    # BEFORE
    # 'phase': 'O5_GetResource', 
    # 'folder': 'csv',
    # 'dataset': '01_output',
    # 'experiment': 'exp_1', 
    # 'date': '2025-09-17',

    # insert also the partition selected
    get_test_path.update({'partition': partition_path['partition']})

    # AFTER
    # 'phase': 'O5_GetResource', 
    # 'folder': 'csv',
    # 'dataset': '01_output',
    # 'experiment': 'exp_1', 
    # 'date': '2025-09-17',
    # 'partition': 0


    ######################################################

    # get test path formatted as string (it's a csv file)
    get_test_path_str = files_handling.path_dict_to_str(get_test_path) + '.csv'

    # create empty file
    with open(get_test_path_str, 'w') as get_partition_test:
        # HEADER
        writer = csv.writer(get_partition_test)
        writer.writerow(["saddr", "uri", "version", "mtype", "token_size", "code", "mid", "token", "options", "payload", "payload_size"])
        get_partition_test.close()
    
    # return coap test path
    return get_test_path



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

        data_to_store = [ip_address,
                        uri,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        e,
                        None
                        ]

        return data_to_store

    else:

        # response payload interpretation
        if isinstance(response.payload, bytes):
            try:
                payload = response.payload.decode("utf-8")
            except UnicodeDecodeError as e:
                print(f"Failed to decode payload: {e}")
                payload = response.payload  # fallback to raw bytes
        else:
            payload = response.payload  # already a str

        options = None

        # if options are defined
        if (len(response.opt._options.keys()) > 0):
            options = {}

            options_key = []
            for option in response.opt._options:
               options_key.append(str(option))

            options_value = []
            for option in response.opt.option_list():
                options_value.append(str(option))

            for i in range(len(options_key)):
                options[options_key[i]] = options_value[i]


        data_to_store = [ip_address,
                        uri,
                        response.version,
                        str(response.mtype),
                        len(response.token),
                        str(response.code),
                        response.mid,
                        response.token.hex(),
                        options,
                        payload,
                        len(response.payload)
                        ]

        return data_to_store # success



# coroutine function: entry point of event loop
async def get_requests(partition_path, start_label, end_label):
    
    
        # OUTPUT FILE: GET results file 
        get_test_path = new_gets_test(partition_path)
        
        # INPUT FILE LABELS
        print(f"Start: {start_label}, End: {end_label}, Partition Size: {end_label - start_label}")


        ######################################################


        physical_path = {
            'phase': partition_path['phase'],
            'folder': partition_path['folder'],
            'dataset': partition_path['dataset'],
            'date': partition_path['date']
        }

        physical_path_str = files_handling.path_dict_to_str(physical_path) + '.csv'

        # 2. open entire date dataset
        with open(physical_path_str, newline='') as zmap_file:

            # interprete the file as csv
            zmap_file_rows = csv.reader(zmap_file)

            # info counter for User Interface
            info_counter = 1

            for index, row in enumerate(zmap_file_rows):
                
                # ignore row indexes lower than start label
                if index < start_label:
                    continue
                
                # ignore row indexes greater or equal than end label
                if index >= end_label:
                    break

                # print status line (# servers left to check)
                print(f"[{info_counter}/{end_label - start_label}] Testing: {row[0]}")
                # updating info_counter
                info_counter += 1

                #########################################
                # When can GET requests be avoided?

                # ----- SUCCESS check -----
                if row[7] == 0: # success = 0/1
                    print(f"\tNot successful response to resource discovery: \n\t\tskip {row[0]}")
                    continue

                # ----- PAYLOAD check -----
                payload = row[11]

                if len(payload) == 0: # empty payload string
                    print(f"\tEmpty payload was returned from resource discovery: \n\t\tskip {row[0]}")
                    continue
            
                #########################################

                # uris = list of uris (no more metadata)
                uris = payload_handling.uri_list_of(payload)

                # ----- </> resource test -----
                if '</>' not in uris:
                    uris.append('</>')


                for i, uri in enumerate(uris):

                    # print status line (# resources left to check)
                    print(f"\t[{i+1}/{len(uris)}] GET request to: {uri}")

                    '''
                    await <COROUTIN_OBJ> 
                    -> await the discovery coroutine, pausing the execution of main until discovery completes
                    '''

                    # resource discovery response
                    response = await get(row[0], uri[1:-1])

                    # debug
                    print(f"\t\tResponse To Store: {response}")
                    
                    files_handling.store_data(response, files_handling.path_dict_to_str(get_test_path) + '.csv')
                
        
        # 3. logs info about current get requests
        experiment = partition_path['experiment']
        date = partition_path['date']
        partition = partition_path['partition']
        dataset = partition_path['dataset']

        log_info = [experiment, date, partition, dataset]

        files_handling.store_data(log_info, "utils/logs/get.csv")