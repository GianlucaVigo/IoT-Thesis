import asyncio
from aiocoap import *

async def test_observability(uri, wait_time=30):
    """Check if a CoAP resource is observable and wait for updates."""
    context = await Context.create_client_context()
    request = Message(code=GET, uri=uri, observe=0)
    pr = context.request(request)

    # Get the initial response
    response = await pr.response
    if response.opt.observe is not None:
        print(f"Resource at {uri} is observable.")
        print(f"Waiting for possible notifications for {wait_time}s...")
        try:
            # Wait for notifications
            async with asyncio.timeout(wait_time):
                async for msg in pr.observation:
                    print("Received notification:", msg.payload.decode())
        except asyncio.TimeoutError:
            print(f"Waited {wait_time}s — stopping observation.")
    else:
        print(f"Resource at {uri} is NOT observable (no Observe option in response).")

    await context.shutdown()

asyncio.run(test_observability("coap://localhost/temperature", wait_time=60))

