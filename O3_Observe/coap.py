import asyncio
import datetime
import pandas as pd

from aiocoap import *
from utils import payload_handling

async def observe_with_timeout(context, saddr, uri, duration):

    print(f"Observation started at: {datetime.datetime.now()} [{uri}]")

    uri_to_check = f"coap://{saddr}:5683{uri}"
    request = Message(code=GET, uri=uri_to_check, observe=0)
    pr = context.request(request)

    observe_responses = {
        'saddr': saddr,
        'uri': uri,
        'duration': duration,
        'updates': []
    }

    async def reader():
        async for response in pr.observation:

            timestamp = datetime.datetime.now()

            observe_responses['updates'].append({
                'code': payload_handling.get_code(response),
                'payload': payload_handling.get_payload(response),
                'timestamp': timestamp
            })

            print(f"Update: \n\tPayload: {payload_handling.get_payload(response)}\n\tTimestamp: {timestamp}")

        print(f"Observation ended.")

    observation_task = asyncio.create_task(reader())

    await asyncio.sleep(duration)
    print(f"\t\tCancelling observation after {duration} seconds [{uri}]")

    pr.observation.cancel()

    try:
        await asyncio.wait_for(observation_task, timeout=5)
    except asyncio.TimeoutError:
        observation_task.cancel()

    if len(observe_responses['updates']) == 0: # empty updates list
        observe_responses['updates'].append("NO UPDATES")

        
    return observe_responses




async def observe_resources(observable_resources_df):
    
    context = await Context.create_client_context()

    tasks = []

    for i, row in observable_resources_df.iterrows():
        task = asyncio.create_task(observe_with_timeout(context, row['saddr'], row['uri'], 300))
        tasks.append(task)

    obs_results = await asyncio.gather(*tasks)

    obs_results_df = pd.DataFrame(obs_results)

    return obs_results_df