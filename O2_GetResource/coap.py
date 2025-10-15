from aiocoap import *
import aiocoap

import asyncio
import pandas as pd
import logging
import time

from utils import payload_handling

# log info settings
logging.basicConfig(level=logging.INFO)


# perform a CoAP GET request of a specified CoAP resource (IP addr + Resource URI)
async def get(ip_address, uri, context, declared_obs, home_path_inserted, progress_counter):

    MAX_TRANSMIT_WAIT = (
        aiocoap.numbers.TransportTuning.ACK_TIMEOUT *
        (2 ** (aiocoap.numbers.TransportTuning.MAX_RETRANSMIT + 1) - 1) *
        aiocoap.numbers.TransportTuning.ACK_RANDOM_FACTOR
    )

    data_to_store = None

    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{uri}"
    # build the request message
    request = Message(code=GET, uri=uri_to_check, observe=0)

    try:
        print(f"\t\t[{progress_counter + 1}] GET request to: {uri}")
        # send the request and obtained the response
        response = await asyncio.wait_for(context.request(request).response, timeout=MAX_TRANSMIT_WAIT + 5)

        await asyncio.sleep(0.05)


        if payload_handling.get_observe(response, uri) == True:  # OBS resource        
            if declared_obs == True:                                #   -> declared OBS = CORRECT
                observable_status = 0
            else:                                                   #   -> NOT declared OBS = WRONG
                observable_status = 1
        else:                                               # NOT OBS resource
            if declared_obs == True:                                #   -> declared OBS = WRONG
                observable_status = 2               
            else:                                                   #   -> NOT declared OBS = CORRECT
                observable_status = 3


        # --------- home path resource managing ---------
        # NB: at the moment I considered only to add '/' resource to get some info about server used
        if uri == '/':
            user_inserted_status = home_path_inserted
        else:
            user_inserted_status = False
        
        data_to_store = {'saddr': ip_address,
                        'uri': uri,
                        'version': payload_handling.get_version(response),
                        'mtype': payload_handling.get_mtype(response),
                        'token-length': payload_handling.get_token_length(response),
                        'code': payload_handling.get_code(response),
                        'mid': payload_handling.get_mid(response),
                        'token': payload_handling.get_token(response),
                        'options': payload_handling.get_options(response),
                        'payload': payload_handling.get_payload(response),
                        'payload-length': payload_handling.get_payload_length(response),
                        'observable': observable_status,
                        'user_inserted': user_inserted_status
        }

    except asyncio.TimeoutError:
        print(f"\t\t\tRequest to {ip_address}{uri} timed out")

        data_to_store = {'saddr': ip_address,
                        'uri': uri,
                        'version': None,
                        'mtype': None,
                        'token-length': None,
                        'code': None,
                        'mid': None,
                        'token': None,
                        'options': None,
                        'payload': 'timeout',
                        'payload-length': None,
                        'observable': None
        }

    except Exception as e:
        print(f"\t\t{e}")
        
        data_to_store = {'saddr': ip_address,
                        'uri': uri,
                        'version': None,
                        'mtype': None,
                        'token-length': None,
                        'code': None,
                        'mid': None,
                        'token': None,
                        'options': None,
                        'payload': e,
                        'payload-length': None,
                        'observable': None
        }

    
    return data_to_store



# coroutine function: entry point of event loop
async def get_requests(discovery_df):

    start_time = time.time()

    total_ips = discovery_df.shape[0]

    responses = []

    for index, row in discovery_df.iterrows():

        # print status line (# servers left to check)
        print(f"\t[{index + 1}/{total_ips}] Testing: {row['saddr']}")
            

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

        # ----- EMPTY PAYLOAD check -----
        if len(row['data']) == 0: # empty payload string
            print(f"\t\tEmpty payload was returned from resource discovery: \n\t\tskip {row['saddr']}")
            continue

        # ----- STILL BINARY PAYLOAD check -----
        if isinstance(row['data'], bytes):
            print(f"\t\tCan't decode its binary payload: \n\t\tskip {row['saddr']}")
            continue

        #########################################

        # modify the MAX_RETRANSMIT from 4 to 2
        aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 2 

        # client context creation
        context = await Context.create_client_context()

        # resources = list of uris + their metadata
        resources = payload_handling.resource_list_of(row['data'])
        # uris = list of uris (no more metadata)
        uris = payload_handling.uri_list_of(row['data'])

        #########################################

        home_path_inserted = True

        for i, uri in enumerate(uris):

            # --------- uris-validity-check ---------
            # 1. uri must start with '/'
            if uri[1] != '/':
                print(f"{uri} does not respect the correct syntax -> NOT EVALUATED")
                del uris[i]
                del resources[i]

            if uri == '</>':
                home_path_inserted = False

        # --------- </> resource test ---------
        if home_path_inserted:
            uris.append('</>')
            resources.append('</>')

        #########################################

        for i, res in enumerate(resources):

            # ------------- observability handling -------------
            declared_obs = payload_handling.get_metadata_value_of(res, 'obs')

            # resource discovery response
            result = await get(row['saddr'], uris[i][1:-1], context, declared_obs, home_path_inserted, i)

            # extending the responses list with the just got ones
            responses.append(result)

        # -------------------------------------
        await asyncio.sleep(0.1)
        await context.shutdown()
        await asyncio.sleep(0.05)

        print(f"Elapsed: {time.time() - start_time}")
    
   
    responses_df = pd.DataFrame(responses)

    return responses_df