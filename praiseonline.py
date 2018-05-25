# -*- coding: UTF-8 -*-
import requests
from ms import cursor,conn
from tool import CloudQQMap

def get_goods_list(access_token,offline_id,count = 5):
    url = "https://open.youzan.com/api/oauthentry/youzan.multistore.goods.delivery/3.0.0/list"
    params = {"access_token":access_token,"offline_id":offline_id}
    r = requests.get(url,params=params)
    if r.status_code == 200 and r.json().get("response"):
        goodsinfo_lis = r.json().get("response").get("list")
        if goodsinfo_lis:
            good_lis = [i["goods_id"] for i in goodsinfo_lis]
            return good_lis
        else:
            return []
    else:
        print("get data err")
        return []


def main():
    cursor.execute("select * from django_aip.third_part_wechat_praisestore where sid  in (15454921)")
    storelis = cursor.fetchall()
    for i in storelis:
        page_size = 100
        access_token = i['access_token']
        sid = i['sid']
        print("-------------------__>",sid)
        offline_url = "https://open.youzan.com/api/oauthentry/youzan.multistore.offline/3.0.0/search"
        params = {"access_token":access_token,"page_size":1,"page_no":1}
        r = requests.get(offline_url,params=params)
        if r.status_code == 200:
            response = r.json().get("response")
            if response:
                offline_count=response['count']
                page_max = offline_count//page_size
                if offline_count%page_size:
                    page_max+=1
                if page_max:
                    for page_no in range(1,page_max+1):
                        params = {"access_token": access_token, "page_size": page_size, "page_no": page_no}
                        r = requests.get(offline_url,params=params)
                        if r.status_code == 200:
                            if r.json().get("response").get("list"):
                                offline_list = r.json().get("response").get("list") or []
                                for offline_info in offline_list:
                                    offline_id = offline_info["id"]
                                    kdt_id = sid
                                    name = offline_info["name"]
                                    province = offline_info["province"]
                                    city = offline_info["city"]
                                    area = offline_info["area"]
                                    address = offline_info["address"]
                                    county_id = offline_info["county_id"]
                                    lng = offline_info["lng"]
                                    lat = offline_info["lat"]
                                    if lng == "0" or lat == "0":
                                        detail_address = (province+city+address).split("（")[0].split("(")[0].encode()
                                        print(detail_address)
                                        Map = CloudQQMap()
                                        lat,lng = Map.getPoint({"address":detail_address})
                                        print(offline_id,lat,lng)
                                    goods_list = get_goods_list(access_token,offline_id)
                                    if goods_list:
                                        goods_list = ",".join(goods_list)
                                    else:
                                        goods_list = None
                                    sql_param = [offline_id,name,province,city,area,address,county_id,lng,
                                                 lat,goods_list,kdt_id]
                                    cursor.execute("replace into django_aip.third_part_wechat_praisestoreonline"
                                                   "(id,`name`,province,city,area,address,county_id,lng,lat,goods_list,"
                                                   "kdt_id)VALUE(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",sql_param)
                                    conn.commit()
        else:
            print("get offline err")


if __name__ == "__main__":
    main()
