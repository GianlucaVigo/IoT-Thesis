import asyncio
from aiocoap import *
import logging


# log info settings
logging.basicConfig(level=logging.INFO)


# perform the resource discovery operation -> GET (./well-known/core)
async def coap_get(ip_address, uri):

    # client context creation
    protocol = await Context.create_client_context()
    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{uri}"
    # build the request message
    request = Message(code=GET, uri=uri_to_check)

    try:
        # send the request and obtained the response
        response = await protocol.request(request).response

        print(f"\n\tCode: {response.code}")
        print(f"\tType: {response.mtype}")
        print(f"\tPayload: {response.payload.decode("utf-8")}")
        print(f"\tPayload Size: {len(response.payload)}")

    except Exception as e:
        print(f"\t{e}")
        return None # error

asyncio.run(coap_get("176.101.247.159","/info"))