import asyncio
from aiocoap import *


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
        print(response.payload)

    except Exception as e:
        print(f"\t{e}")

asyncio.run(discovery('27.192.224.10'))