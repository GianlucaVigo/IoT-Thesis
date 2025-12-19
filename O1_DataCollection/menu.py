import datetime
import time

from utils.zmap_handling import before_zmap_execution, execute_zmap, after_zmap_execution
from O1_DataCollection import coap

# orchestration function
def data_collection():
    
    # 1. master date -> zmap + decode res + gets
    cidr_id, cidr = before_zmap_execution()
    
    start = execute_zmap(cidr_id, cidr)
    
    after_zmap_execution(cidr_id)
    
    # debug 
    start = datetime.datetime.now()
    
    # 2. subsequent dates -> aiocoap gets
    # start
    print('start: ', start)

    for i, offset in enumerate([10, 20, 30, 40, 50, 60], start=1):
        delay = (datetime.timedelta(seconds=offset) - (datetime.datetime.now() - start)).total_seconds()
        delay = max(0, delay)  # prevent negative delays

        time.sleep(delay)
        gets(i)
    
    return



def gets(number):
    
    print(f"gets - {number} - function executed!")
    print('\t', datetime.datetime.now())
    
    return