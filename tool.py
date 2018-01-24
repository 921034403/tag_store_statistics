import time,random
from datetime import datetime,timedelta
import calendar

def getFirstDayOfCurMonth():
    d = datetime.now().strftime('%Y-%m')
    d += '-01'
    return d

def getFirstDayOfCurYear():
    d = datetime.now().strftime('%Y')
    d += '-01-01'
    return d

def getFirstDayOfCurWeek():
    d = datetime.now().strftime('%Y-%m-%d')
    d= datetime.strptime(d,'%Y-%m-%d')
    delta = timedelta(days=-(d.weekday()))
    _d = d+delta
    _d = _d.strftime('%Y-%m-%d')
    return _d

def getCurDay():
    d = datetime.now()
    _d = d.strftime('%Y-%m-%d')
    return _d

def getEveryCycleLastDay(cycle_desc,statistics_type):
    if statistics_type == 2:
        d = calendar.monthrange(int(cycle_desc),12)[-1]
        return cycle_desc+'-'+'12'+'-'+str(d)
    elif statistics_type == 3:
        d = calendar.monthrange(int(cycle_desc[:4]),int(cycle_desc[5:7]))[-1]
        return cycle_desc+'-'+str(d)
    elif statistics_type == 4:
        rand_date = cycle_desc[:4]+'-02-01'
        year,weekcount,weekday =  datetime.strptime(rand_date,'%Y-%m-%d').isocalendar()
        _d = datetime.strptime(rand_date,'%Y-%m-%d')
        delta = timedelta(days=7-weekday)
        ref_date = _d+delta
        delta_days = timedelta(days=7*(int(cycle_desc[5:7])-weekcount))
        d = ref_date +  delta_days
        d = d.strftime('%Y-%m-%d')
        return d
    elif statistics_type == 5:
        return cycle_desc









