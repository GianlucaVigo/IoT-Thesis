import datetime
from threading import Timer

def func(a, b):
    print('\t', datetime.datetime.now())
    return a * b

# start
start = datetime.datetime.now()
print(start)

# Schedule a timer for 5 seconds
# We pass arguments 3 and 4
Timer((datetime.timedelta(seconds=10) - (datetime.datetime.now() - start)).seconds, func, [3, 4]).start()
