import datetime

from threading import Timer

from utils.zmap_handling import before_zmap_execution, execute_zmap, after_zmap_execution
from O1_DataCollection import coap

# orchestration function
def data_collection():
    
    # 1. master date -> zmap + decode res + gets
    cidr_id, cidr = before_zmap_execution()
    
    start = execute_zmap(cidr_id, cidr)
    
    after_zmap_execution(cidr_id)
    
    
    # 2. subsequent dates -> aiocoap gets
    # start
    print('start: ', start)

    Timer((datetime.timedelta(seconds=10) - (datetime.datetime.now() - start)).seconds, gets, 1).start()
    Timer((datetime.timedelta(seconds=20) - (datetime.datetime.now() - start)).seconds, gets, 2).start()
    Timer((datetime.timedelta(seconds=30) - (datetime.datetime.now() - start)).seconds, gets, 3).start()
    Timer((datetime.timedelta(seconds=40) - (datetime.datetime.now() - start)).seconds, gets, 4).start()
    Timer((datetime.timedelta(seconds=50) - (datetime.datetime.now() - start)).seconds, gets, 5).start()
    Timer((datetime.timedelta(seconds=60) - (datetime.datetime.now() - start)).seconds, gets, 6).start()
    
    return



def gets(number):
    
    print(f"gets - {number} - function executed!")
    
    return