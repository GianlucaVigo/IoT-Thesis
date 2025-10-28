import asyncio
from aiocoap import *
from aiocoap import message, Message, GET, CON


async def discovery(ip_address):
    # client context creation
    protocol = await Context.create_client_context()
    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683/gw/query/DiagnotorConnection"
    # build the request message
    request = Message(
        code=GET,
        mtype=CON,
        mid=12345,
        uri=uri_to_check)

    encoded = message.Message.encode(request)
    print(encoded.hex())


    try:
        # send the request and obtained the response
        response = await protocol.request(request).response
        print(response.payload.decode(errors='ignore'))

    except Exception as e:
        print(f"\t{e}")

asyncio.run(discovery('111.59.22.208'))