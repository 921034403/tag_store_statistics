# _*_ coding:utf-8 _*_
from datetime import datetime,timedelta

a='2017-12-31'
b='2018-01-09'

_a = datetime.strptime(a,'%Y-%m-%d')
_b = datetime.strptime(b,'%Y-%m-%d')

_c = _b - _a


# 检测年
def func(max,now):
    now_firstday = now[:4]+'-01-01'
    date_now = datetime.strptime(now_firstday,'%Y-%m-%d')
    delta = timedelta(days=-1)
    date_now += delta
    date_now = date_now.strftime('%Y-%m-%d')
    if date_now == max:
        return False
    else:
        return True



print(func(a,b))