import asyncio
import pandas as pd
import time
import aiocoap
import logging

from aiocoap import *

from utils import payload_handling, workflow_handling

################################################################################################

# log info settings
logging.basicConfig(level=logging.CRITICAL)

SEMAPHORE_SLOTS = 1000

################################################################################################

# perform a CoAP GET request of a specified CoAP resource (IP addr + Resource URI)
async def get(ip_address, uri, context, declared_obs, user_inserted, semaphore):
        
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
            'data_format': None,
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
            print(f"\t[{ip_address}] ({time.ctime(time.time())})\tGET request to: {uri}")
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
                'data_format': payload_handling.get_payload_format(response),   
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

################################################################################################

async def coap(discovery_df, is_discovery):

    # modify the MAX_RETRANSMIT from 4 to 2
    aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 2

    # semaphore instantiation
    semaphore = asyncio.Semaphore(SEMAPHORE_SLOTS)

    # client context creation
    context = await Context.create_client_context()

    await asyncio.sleep(2)
    
    # -------------------------------------
    
    if is_discovery:
        responses_df = await discovery(discovery_df, semaphore, context)
    else:
        responses_df = await get_requests(discovery_df, semaphore, context)
    
    # -------------------------------------

    await context.shutdown()

    return responses_df

################################################################################################

async def discovery(discovery_df, semaphore, context):
    
    tasks = []
    
    for _, row in discovery_df.iterrows():
        
        if row['success'] == 1:
            tasks.append(get(row['saddr'], '/.well-known/core', context, False, False, semaphore))
        
    responses = await asyncio.gather(*tasks)
    
    responses_df = pd.DataFrame(responses)
    
    return responses_df

################################################################################################

async def get_requests(discovery_df, semaphore, context):
    
    tasks = []

    num_of_tasks = 0

    #########################################

    for _,row in discovery_df.iterrows():

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


            for _, res in enumerate(resources):
                
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

                # ---------------------------------------
                
                tasks.append(get(row['saddr'], uri, context, declared_obs, user_inserted, semaphore))
                num_of_tasks += 1


    # collect ALL results
    results = await asyncio.gather(*tasks)

    # convert list to DataFrame
    responses_df = pd.DataFrame(results)
            
    # -------------------------------------
    
    # Number of examined resources 
    print(f"Number of tasks: {num_of_tasks}")
    

    return responses_df