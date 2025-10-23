import asyncio
import time
import struct

from aiocoap import *
from datetime import datetime, timezone

async def efento_bytes_to_timestamp(raw_bytes):
    """
    Convert 4-byte Efento binary timestamp (big-endian) to datetime.
    """
    if len(raw_bytes) != 4:
        raise ValueError("Input must be exactly 4 bytes long.")
    
    # Interpret bytes as big-endian unsigned integer
    unix_time = struct.unpack(">I", raw_bytes)[0]

    # Convert to datetime
    dt_utc = datetime.fromtimestamp(unix_time, tz=timezone.utc)
    dt_local = dt_utc.astimezone()  # Convert to local time zone

    return unix_time, dt_utc, dt_local


async def get(context, uri, name):

    print(f"Polling to {name} executed at [{time.ctime(time.time())}]")

    # build the request message
    request = Message(code=GET, uri=uri)

    try:
        # send the request and obtained the response
        response = await context.request(request).response
        print(f"{name}: {response.payload} - [{time.ctime(time.time())}]")

        print("Efento Binary → Unix Timestamp Conversion\n" + "="*45)

        unix_time, dt_utc, dt_local = await efento_bytes_to_timestamp(response.payload)
        print(f"Raw: {response.payload}  |  Unix: {unix_time}")
        print(f"UTC:   {dt_utc.isoformat()}")
        print(f"Local: {dt_local.isoformat()}")
        print("-" * 45)

    except Exception as e:
        print(f"\t{e}")

    return


async def main():

    context = await Context.create_client_context()

    while(True):

        await asyncio.gather(
            get(context, "coap://129.232.178.234:5683/efento/t", "Efento_1"),
            get(context, "coap://157.245.204.199:5683/efento/t", "Efento_2"),
            get(context, "coap://165.232.144.56:5683/efento/t", "Efento_3"),
            get(context, "coap://195.201.229.48:5683/efento/t", "Efento_4"),
            get(context, "coap://203.158.192.168:5683/efento/t", "Efento_5"),
        )
        print('°' * 50)
        time.sleep(0.05)
    return


asyncio.run(main())