'''import asyncio
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

asyncio.run(coap_get("120.238.223.180","/device/command/log"))
'''

import asyncio
from aiocoap import Context, Message, GET

async def observe_resource(uri: str):
    protocol = await Context.create_client_context()

    # Send GET with observe=0 to start observation
    request = Message(code=GET, uri=uri, observe=0)
    pr = protocol.request(request)

    try:
        # Handle initial response
        first_response = await pr.response
        print("First response:", first_response.payload.decode())
        print("Observe option in response:", first_response.opt.observe)
    except Exception as e:
        print("Failed to fetch resource:", e)
        return

    # Iterate over observation updates
    async for notification in pr.observation:
        if notification.code.is_successful():
            print("Notification:", notification.payload.decode())
        else:
            print("Error from server:", notification.code)

if __name__ == "__main__":
    ip_addr = "183.238.203.114"
    uri = "/device/inform/data"
    address = f"coap://{ip_addr}:5683{uri}"
    asyncio.run(observe_resource(address))



    