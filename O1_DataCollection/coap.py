import asyncio
import pandas as pd
import aiocoap
import datetime

from aiocoap import *

from utils import payload_handling, workflow_handling

################################################################################################

def get_max_transmit_wait():
    return (
        aiocoap.numbers.TransportTuning.ACK_TIMEOUT *
        (2 ** (aiocoap.numbers.TransportTuning.MAX_RETRANSMIT + 1) - 1) *
        aiocoap.numbers.TransportTuning.ACK_RANDOM_FACTOR
    )

################################################################################################

# perform a CoAP GET request of a specified CoAP resource (IP addr + Resource URI)
async def get(ip_address, uri, context, declared_obs, user_inserted, timeout_time, must_test_obs):
        
    data_to_store = {
        'saddr': ip_address,
        'uri': uri,
        'version': None,
        'mtype': None,
        'token': None,
        'token_length': None,
        'code': None,
        'mid': None,
        'options': None,
        'observable': None,
        'data': None,
        'data_format': None,
        'data_length': None,
        'user_inserted': None
    }

    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{uri}"
    
    # build the request message
    if must_test_obs:
        request = Message(code=GET, uri=uri_to_check, observe=0)
    else:
        request = Message(code=GET, uri=uri_to_check)
        

    try:
        
        # send the request and obtained the response
        response = await asyncio.wait_for(context.request(request).response, timeout=timeout_time)


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

        data_to_store.update({
            'data': 'timeout'
        })

    except Exception as e:
                
        data_to_store.update({
            'data': e
        })

    return data_to_store

################################################################################################

async def coap(discovery_df, operation_type):

    # -------------------------------------
    match operation_type:
        case 0:
            # setting MAX_RETRANSMIT = 3
            aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 3
            responses_df = await discovery(discovery_df)
        case 1:
            # setting MAX_RETRANSMIT = 1
            aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 1
            responses_df = await get_requests(discovery_df)
        case 2:
            # setting MAX_RETRANSMIT = 3
            aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 3
            responses_df = await get_requests_to_observable_resources(discovery_df)
    
    # -------------------------------------

    return responses_df

################################################################################################

async def worker(worker_id, queue, results, rate_delay):
    
    context = await Context.create_client_context()
    await asyncio.sleep(1)
    
    last_logged = None

    while True:
        
        size = queue.qsize()
        milestone = size // 500

        if milestone != last_logged and worker_id == 0:
            print(f"\tQueue size ~ {milestone * 500}")
            last_logged = milestone
        
        item = await queue.get()
        
        if item is None:
            queue.task_done()
            break

        ip, uri, declared_obs, user_inserted, timeout, must_test_obs = item

        result = await get(
            ip, uri,
            context,
            declared_obs,
            user_inserted,
            timeout,
            must_test_obs
        )

        results.append(result)

        # rate limiting
        await asyncio.sleep(rate_delay)

        queue.task_done()

    await context.shutdown()
    
################################################################################################

async def get_requests_to_observable_resources(observable_df):
    
    print("\tMax Retransmissions: ", aiocoap.numbers.TransportTuning.MAX_RETRANSMIT)
    timeout = get_max_transmit_wait() + 5
    print("\tTimeout: ", timeout)
    
    # ---------------------------------------
    
    queue = asyncio.Queue()
    results = []

    NUM_WORKERS = 15
    RATE_DELAY = 0.21

    workers = [
        asyncio.create_task(worker(i, queue, results, RATE_DELAY))
        for i in range(NUM_WORKERS)
    ]

    for _, row in observable_df.iterrows():
        
        if row['observable'] == 0:
            declared_obs = True
        elif row['observable'] == 1:
            declared_obs = False
        
        await queue.put((row['saddr'], row['uri'], declared_obs, False, timeout, False))
    
    for _ in workers:
        await queue.put(None)

    await queue.join()
    await asyncio.gather(*workers)
    
    return pd.DataFrame(results)

################################################################################################

async def discovery(discovery_df):
    
    print("\tMax Retransmissions: ", aiocoap.numbers.TransportTuning.MAX_RETRANSMIT)
    timeout = get_max_transmit_wait() + 5
    print("\tTimeout: ", timeout)
    
    # ---------------------------------------
    
    queue = asyncio.Queue()
    results = []

    NUM_WORKERS = 15
    RATE_DELAY = 0.21

    workers = [
        asyncio.create_task(worker(i, queue, results, RATE_DELAY))
        for i in range(NUM_WORKERS)
    ]

    for _, row in discovery_df.iterrows():
        
        await queue.put((row['saddr'], '/.well-known/core', False, False, timeout, False))
    
    for _ in workers:
        await queue.put(None)

    await queue.join()
    await asyncio.gather(*workers)
    
    return pd.DataFrame(results)

################################################################################################

async def get_requests(discovery_df):
    
    print("\tMax Retransmissions: ", aiocoap.numbers.TransportTuning.MAX_RETRANSMIT)
    timeout = get_max_transmit_wait() + 5
    print("\tTimeout: ", timeout)
    
    # ---------------------------------------
    
    queue = asyncio.Queue()
    results = []

    NUM_WORKERS = 15
    RATE_DELAY = 0.21

    # create NUM_WORKERS workers
    workers = [
        asyncio.create_task(worker(i, queue, results, RATE_DELAY))
        for i in range(NUM_WORKERS)
    ]

    for _, row in discovery_df.iterrows():
            
        if workflow_handling.avoid_get(row):
            continue

        resources = payload_handling.resource_list_of(row['data'])

        home_path_inserted = False
        if '</>' not in row['data']:
            resources.append('</>')
            home_path_inserted = True

        for res in resources:
            uri = res.split(';')[0].strip('<>')
            if not uri.startswith('/'):
                continue

            declared_obs = payload_handling.get_metadata_value_of(res, 'obs')
            user_inserted = (uri == '/' and home_path_inserted)

            await queue.put((row['saddr'], uri, declared_obs, user_inserted, timeout, True))
    
    # stop workers
    for _ in workers:
        await queue.put(None)

    await queue.join()
    await asyncio.gather(*workers)
    
    return pd.DataFrame(results)