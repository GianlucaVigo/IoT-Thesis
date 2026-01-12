import asyncio
import pandas as pd
import aiocoap

from aiocoap import *

from utils import payload_handling, workflow_handling

################################################################################################

SEMAPHORE_SLOTS = 50

# setting MAX_RETRANSMIT = 2
aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 2

MAX_TRANSMIT_WAIT = (
            aiocoap.numbers.TransportTuning.ACK_TIMEOUT *
            (2 ** (aiocoap.numbers.TransportTuning.MAX_RETRANSMIT + 1) - 1) *
            aiocoap.numbers.TransportTuning.ACK_RANDOM_FACTOR
        )

################################################################################################

# perform a CoAP GET request of a specified CoAP resource (IP addr + Resource URI)
async def get(ip_address, uri, context, declared_obs, user_inserted, semaphore):
        
    async with semaphore:
        
        # prevent packet burst
        await asyncio.sleep(0.01)
        
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

        # resource URI to be checked
        uri_to_check = f"coap://{ip_address}:5683{uri}"
        # build the request message
        request = Message(code=GET, uri=uri_to_check, observe=0)

        try:
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

            decoded_message_payload = payload_handling.get_payload(response)

            data_to_store.update({
                'version': payload_handling.get_version(response),
                'mtype': payload_handling.get_mtype(response),
                'token_length': payload_handling.get_token_length(response),
                'code': payload_handling.get_code(response),
                'mid': payload_handling.get_mid(response),
                'token': payload_handling.get_token(response),
                'options': payload_handling.get_options(response),
                'data': decoded_message_payload,
                'data_format': payload_handling.get_payload_format(decoded_message_payload),   
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

async def coap(discovery_df, operation_type):

    # semaphore instantiation
    semaphore = asyncio.BoundedSemaphore(SEMAPHORE_SLOTS)

    # client context creation
    context = await Context.create_client_context()

    await asyncio.sleep(2)
    
    # -------------------------------------
    match operation_type:
        case 0:
            responses_df = await discovery(discovery_df, semaphore, context)
        case 1:
            responses_df = await get_requests(discovery_df, semaphore, context)
        case 2:
            responses_df = await get_requests_to_observable_resources(discovery_df, semaphore, context)
    
    # -------------------------------------

    await context.shutdown()

    return responses_df


################################################################################################

async def get_requests_to_observable_resources(observable_df, semaphore, context):
    
    tasks = []
    
    responses = []
    
    #########################################
    
    for _, row in observable_df.iterrows():
            
        if len(tasks) == 100:
                
            # execute get requests in batches of size 100
            responses.extend(await asyncio.gather(*tasks))
                
            # empty the tasks list
            tasks.clear()
                
        tasks.append(get(row['saddr'], row['uri'], context, False, False, semaphore))
    
    # last batch
    if tasks:
        # execute get requests in batches of size 100
        responses.extend(await asyncio.gather(*tasks))
    
    responses_df = pd.DataFrame(responses)
    
    return responses_df

################################################################################################

async def discovery(discovery_df, semaphore, context):
    
    tasks = []
    
    responses = []
    
    #########################################
    
    for _, row in discovery_df.iterrows():
        
        if row['success'] == 1:
            
            if len(tasks) == 100:
                
                # execute get requests in batches of size 100
                responses.extend(await asyncio.gather(*tasks))
                
                # empty the tasks list
                tasks.clear()
                
            tasks.append(get(row['saddr'], '/.well-known/core', context, False, False, semaphore))
    
    # last batch
    if tasks:
        # execute get requests in batches of size 100
        responses.extend(await asyncio.gather(*tasks))
    
    responses_df = pd.DataFrame(responses)
    
    return responses_df

################################################################################################

async def execute_get_tasks(resources_dictionary, context, semaphore):
    
    responses = []
    
    tasks = []
    
    for uri in resources_dictionary.keys():
        
        for index in range(len(resources_dictionary[uri]['saddr'])):
            
            saddr = resources_dictionary[uri]['saddr'][index]
            declared_obs = resources_dictionary[uri]['declared_obs'][index]
            user_inserted = resources_dictionary[uri]['user_inserted'][index]
            
            tasks.append(get(saddr, uri, context, declared_obs, user_inserted, semaphore))
            
            if len(tasks) == 100:
                responses.extend(await asyncio.gather(*tasks))
                tasks.clear()
                
    if tasks:
        responses.extend(await asyncio.gather(*tasks))
        tasks.clear()
    
    return responses

################################################################################################

async def get_requests(discovery_df, semaphore, context):
    
    responses = []
    
    resources_dict = {}
    
    to_be_evaluated_resources = 0

    #########################################

    for _,row in discovery_df.iterrows():

        if not workflow_handling.avoid_get(row):

            # resources = list of uris + their metadata
            resources = payload_handling.resource_list_of(row['data'])

            #########################################

            home_path_inserted = False
            # home path resource is not present in the resource list
            if row['data'].find('</>') == -1:
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
                
                if uri not in resources_dict.keys():
                    resources_dict[uri] = {
                        'saddr': [row['saddr']], 
                        'user_inserted': [user_inserted],
                        'declared_obs': [declared_obs]
                    }
                    
                else:
                    resources_dict[uri]['saddr'].extend([row['saddr']]) 
                    resources_dict[uri]['user_inserted'].extend([user_inserted]) 
                    resources_dict[uri]['declared_obs'].extend([declared_obs])
                    
                to_be_evaluated_resources += 1
                    
                    
                if to_be_evaluated_resources == 10000:
                    
                    responses.extend(await execute_get_tasks(resources_dict, context, semaphore))
                    
                    # empty the tasks list
                    resources_dict.clear()
                    to_be_evaluated_resources = 0
                

    # last batch
    if to_be_evaluated_resources > 0:
        
        # execute get requests in batches of size 100
        responses.extend(await execute_get_tasks(resources_dict, context, semaphore))
               
    # convert list to DataFrame
    responses_df = pd.DataFrame(responses)

    return responses_df