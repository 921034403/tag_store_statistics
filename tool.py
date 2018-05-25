# _*_ coding:utf-8 _*_
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


def isFullCycle(max,now,cycle):
    if max==None:
        return False
    if type(max)==str:
        pass
    else:
        max = max.strftime('%Y-%m-%d')
    if cycle == '年':
        now_firstday = now[:4]+'-01-01'
    else:
        now_firstday = now
    date_now = datetime.strptime(now_firstday,'%Y-%m-%d')
    delta = timedelta(days=-1)
    date_now += delta
    date_now = date_now.strftime('%Y-%m-%d')
    if date_now == max:
        # print('%s --->不足一个周期'%(cycle))
        return False
    else:
        return True

class CloudQQMap(object):
    def __init__(self):
        self.r = 6371393
        self.url = "http://apis.map.qq.com"
        QQ_Map_Key = "35DBZ-GJZKK-NAXJ7-AC6BO-VLFJH-BVBKG"
        QQ_Map_Sk = "5hNIWW8zIwHmKoHREgQDaEXI7PPn9FqB"
        self.key = QQ_Map_Key
        self.sk = QQ_Map_Sk

    def caculateAKSN(self):
        querystring = [k+"="+self.params[k] for k in self.params]
        querystring = ("&".join(querystring))
        uri = self.uri+"?"+urllib.quote(querystring,safe="%/:=&?~#+!$,;'@()*[]|")
        str = urllib.quote_plus(uri+self.sk)
        sn = hashlib.md5(str).hexdigest()
        return uri,sn

    def setCity(self,params,uri="/ws/geocoder/v1/"):
        '''
        :param uri:     请求的路由
        :param params:  请求的参数字典
        '''
        self.lat = params["location"].split(",")[0]
        self.lng = params["location"].split(",")[1]
        self.uri = uri
        self.params = params
        self.params["key"] = self.key
        uri,sn = self.caculateAKSN()
        url = self.url+uri+"&sn="+sn
        r = requests.get(url)
        if r.json().get("status") == 0:
            print(r.json()["result"]["address"]+r.json()["result"]["formatted_addresses"]["recommend"])
            self.city = r.json()["result"]["address_component"]["city"]
        else:
            self.city = None

    # 获取网点的坐标
    def getPoint(self,params,uri="/ws/geocoder/v1/"):
        self.uri = uri
        self.params = params
        self.params["key"] = self.key
        uri, sn = self.caculateAKSN()
        url = self.url + uri + "&sn=" + sn
        r = requests.get(url)

        if r.json().get("status") == 0:
            result = r.json()["result"]
            return result["location"]["lat"],result["location"]["lng"]
        else:
            return None,None

    # 返回驾车距离最近的网点，否则返回None
    def minDistance(self,offlineLis):
        if not self.city:
            print("获取用户地址信息出错")
            return None
        self.uri = "/ws/distance/v1/"
        self.params = {"to":self.lat+","+self.lng}
        self.params["key"] = self.key
        for offlineinfo in offlineLis:
            self.params["from"] = offlineinfo["lat"]+","+offlineinfo["lng"]
            uri, sn = self.caculateAKSN()
            url = self.url + uri + "&sn=" + sn
            r = requests.get(url)
            if r.json()["status"] == 0:
                result = r.json().get("result")
                if result:
                    offlineinfo["distance"] = result["elements"][0]["distance"]
                else:
                    offlineinfo["distance"] = 10000000000
        offlineLis = [i for i in offlineLis if i.get("distance")]
        if offlineLis:
            offlineLis.sort(key=lambda i:i["distance"])
            return offlineLis[0]["goods_list"]
        else:
            return None

    # 返回直线距离最近的网点，否则返回None
    def minDistance1(self,offlineLis):
        for i in offlineLis:
            lat1, lng1,lng2, lat2 = float(self.lat),float(self.lng),float(i["lng"]),float(i["lat"])
            lat1, lng1,lng2, lat2 = map(radians, [lat1,lng1,lng2, lat2])
            dlon =lng2-lng1
            dlat =lat2-lat1
            a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
            c = 2 * asin(sqrt(a))
            minD =  c * self.r
            i["distance"] = minD
        if offlineLis:
            offlineLis.sort(key=lambda i: i["distance"])
            return offlineLis[0]["goods_list"]
        else:
            return None




