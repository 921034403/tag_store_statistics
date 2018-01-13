import time,random


def get_store_code():
    time1 = int(time.time()*10000000)
    store_code = 'SH'+str(time1)
    return store_code



