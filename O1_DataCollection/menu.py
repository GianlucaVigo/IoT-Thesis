import datetime
import time

from O1_DataCollection.lookups import lookups
from O1_DataCollection.zmap import zmap

# orchestration function
def data_collection():
    
    
    # 1. master date -> execute zmap
    
    cidr_id, start = zmap()

    
    # 2. subsequent dates -> aiocoap gets
    
    SECONDS_PER_DAY = 10 * 60
    
    for i, offset in enumerate([1, 2, 3], start=1):
        delay = (datetime.timedelta(seconds=offset * SECONDS_PER_DAY) - (datetime.datetime.now() - start)).total_seconds()
        delay = max(0, delay)  # prevent negative delays
        
        print('-'*30)
        print(f"[{i}] Waiting for {delay} seconds ({datetime.datetime.now()})")
        print('-'*30)

        time.sleep(delay)
        
        print('Time: ', datetime.datetime.now())
        
        lookups(cidr_id)
    
    return