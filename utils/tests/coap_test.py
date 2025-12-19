import asyncio
from aiocoap import *
import logging


# log info settings
logging.basicConfig(level=logging.INFO)


# perform the resource discovery operation -> GET (/.well-known/core)
async def coap_get():


    # build the request message
    request = Message(mtype=CON, code=GET, mid=1234)
    request.opt.uri_path = (".well-known", "core")
    request.token = b'\x01\x02\x03\x04'
    
    raw_bytes = request.encode()
    
    hex_string = raw_bytes.hex()
    
    print(hex_string)
    
    return




asyncio.run(coap_get())

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

'''
'''import asyncio
from aiocoap import *

async def check_blockwise(uri):
    protocol = await Context.create_client_context()
    
    request = Message(code=GET, uri=uri)
    
    try:
        response = await protocol.request(request).response
        
        print(f"Response code: {response.code}")
        print(f"Payload length: {len(response.payload)} bytes")
        print(f"Payload:\n{response.payload.decode(errors='ignore')}\n")
        
        # Check for Block2 option
        block2 = response.opt.block2
        if block2:
            print(f"Block2 detected!")
            print(f"  Block number: {block2.block_number}")
            print(f"  More blocks flag: {block2.more}")
            print(f"  Block size: {block2.size} bytes")
        else:
            print("No Block2 option detected. Full payload in single message.")

        
        # response.opt is an Options object
        options_dict = {}

        # Iterate over all options
        for option in response.opt.option_list():
            # option.number = CoAP option number
            # option.value = option value
            options_dict[str(option.number)] = str(option.value)

        print(options_dict)

        print(response.opt)
    
    except Exception as e:
        print(f"Failed to fetch resource: {e}")

if __name__ == "__main__":
    uri = "coap://111.61.24.150/.well-known/core"  # replace with your server
    asyncio.run(check_blockwise(uri))'''