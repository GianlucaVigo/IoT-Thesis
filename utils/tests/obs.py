import asyncio
from aiocoap import *

async def observe_with_timeout(context, uri, name, duration=10):

    print(f"Observation on {name} has started!")

    """Observe a resource and cancel after a given duration"""
    request = Message(code=GET, uri=uri, observe=0)
    pr = context.request(request)

    # Start the observation task
    async def reader():
        async for response in pr.observation:
            print(f"[{name}] Update: {response.payload.decode()}")
        print(f"[{name}] Observation ended.")

    # Run observation + timer concurrently
    observation_task = asyncio.create_task(reader())

    await asyncio.sleep(duration)
    print(f"[{name}] Cancelling observation after {duration} seconds.")
    pr.observation.cancel()  # Stop observing
    await observation_task   # Wait for it to finish cleanly

async def main():
    context = await Context.create_client_context()

    await asyncio.gather(
        observe_with_timeout(context, "coap://129.232.178.234:5683/efento/t", "Efento_1", duration=600),
        observe_with_timeout(context, "coap://157.245.204.199:5683/efento/t", "Efento_2", duration=600),
    )

asyncio.run(main())
