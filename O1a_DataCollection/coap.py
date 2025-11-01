from aiocoap import *
import aiocoap

import asyncio
import pandas as pd
import logging
import time

from utils import payload_handling, workflow_handling

# log info settings
logging.basicConfig(level=logging.INFO)

CONCURRENCY = 30


# perform a CoAP GET request of a specified CoAP resource (IP addr + Resource URI)
async def get(ip_address, uri, context, declared_obs, user_inserted, progress_counter, semaphore):
        
    async with semaphore:
        
        data_to_store = {
            'saddr': ip_address,
            'uri': uri,
            'version': None,
            'mtype': None,
            'token_length': None,
            'code': None,
            'mid': None,
            'token': None,
            'options': None,
            'data': None,
            'data_length': None,
            'observable': None,
            'user_inserted': None
        }

        MAX_TRANSMIT_WAIT = (
            aiocoap.numbers.TransportTuning.ACK_TIMEOUT *
            (2 ** (aiocoap.numbers.TransportTuning.MAX_RETRANSMIT + 1) - 1) *
            aiocoap.numbers.TransportTuning.ACK_RANDOM_FACTOR
        )

        # resource URI to be checked
        uri_to_check = f"coap://{ip_address}:5683{uri}"
        # build the request message
        request = Message(code=GET, uri=uri_to_check, observe=0)

        try:
            print(f"\t\t[{"{:2d}".format(progress_counter + 1)}] ({time.ctime(time.time())})\tGET request to: {uri}")
            # send the request and obtained the response
            response = await asyncio.wait_for(context.request(request).response, timeout=MAX_TRANSMIT_WAIT + 5)


            # -------- observability handling --------
            # OBS resource
            if payload_handling.get_observe(response, uri) == True:
                
                #   -> declared OBS = CORRECT          
                if declared_obs == True:                                
                    observable_status = 0
                    
                #   -> NOT declared OBS = WRONG
                else:                                                   
                    observable_status = 1
                    
            # NOT OBS resource
            else:
                
                #   -> declared OBS = WRONG                                               
                if declared_obs == True:                                
                    observable_status = 2
                    
                #   -> NOT declared OBS = CORRECT               
                else:                                                   
                    observable_status = 3
            # ------------------------------------------

                
            data_to_store.update({
                'version': payload_handling.get_version(response),
                'mtype': payload_handling.get_mtype(response),
                'token_length': payload_handling.get_token_length(response),
                'code': payload_handling.get_code(response),
                'mid': payload_handling.get_mid(response),
                'token': payload_handling.get_token(response),
                'options': payload_handling.get_options(response),
                'data': payload_handling.get_payload(response),
                'data_length': payload_handling.get_payload_length(response),
                'observable': observable_status,
                'user_inserted': user_inserted
            })

        except asyncio.TimeoutError:
            print(f"\t\t\tRequest to {ip_address}{uri} timed out")

            data_to_store.update({
                'data': 'timeout'
            })

        except Exception as e:
            print(f"\t\t{e}")
                
            data_to_store.update({
                'data': e
            })

        return data_to_store



# coroutine function: entry point of event loop
async def get_requests(discovery_df):

    start_time = time.time()

    total_ips = discovery_df.shape[0]

    responses = []

    # modify the MAX_RETRANSMIT from 4 to 2
    aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 2

    semaphore = asyncio.Semaphore(CONCURRENCY)

    # client context creation
    contexts = [await Context.create_client_context() for _ in range(CONCURRENCY)]

    await asyncio.sleep(2)

    for index, row in discovery_df.iterrows():

        # print status line (# servers left to check)
        print(f"\t[{index + 1}/{total_ips}] Testing: {row['saddr']}")
            
        if not workflow_handling.avoid_get(row):

            # resources = list of uris + their metadata
            resources = payload_handling.resource_list_of(row['data'])
            # uris = list of uris (no more metadata)
            uris = payload_handling.uri_list_of(row['data'])

            #########################################

            home_path_inserted = False
            # home path resource is not present in the resource list
            if row['data'].find('</>') == -1:
                uris.append('</>')
                resources.append('</>')
                home_path_inserted = True


            tasks = []

            for i, res in enumerate(resources):
                
                uri = res.split(';')[0].strip('<>')

                # --------- uris-validity-check ---------
                # 1. uri must start with '/'
                if not uri.startswith('/'):
                    print(f"{uri} does not respect the correct syntax -> NOT EVALUATED")
                    continue
                # ---------------------------------------

                # ------------- observability handling -------------
                declared_obs = payload_handling.get_metadata_value_of(res, 'obs')
                user_inserted = False
                
                if uri == '/':
                    user_inserted = home_path_inserted
                
                slot_idx= i % CONCURRENCY
                context = contexts[slot_idx]

                tasks.append(get(row['saddr'], uri, context, declared_obs, user_inserted, i, semaphore))


            results = await asyncio.gather(*tasks)

            responses.extend(results)

    # -------------------------------------

    for ctx in contexts:
        await ctx.shutdown()

    
    print(f"Elapsed: {time.time() - start_time}")
    
    responses_df = pd.DataFrame(responses)

    return responses_df