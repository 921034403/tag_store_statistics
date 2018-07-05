# _*_ coding:utf-8 _*_



from ms import cursor,conn,praise_es
from sync_goods_conf import conf
import requests,datetime,random
from es_config import client
from elasticsearch_dsl import Search
from elasticsearch import helpers





class SyncGoods(object):
    def __init__(self,kdt_id=[]):
        if kdt_id:
            cursor.execute("select sid,`name`,access_token from third_part_wechat_praisestore "
                           "where sid in %s",[kdt_id])
        else:
            cursor.execute("select sid,`name`,access_token from third_part_wechat_praisestore")
        self.stores = cursor.fetchall()
        self.page_size  = 100
        self.item       = "https://open.youzan.com/api/oauthentry/youzan.item/3.0.0/get"
        self.onsale     = "https://open.youzan.com/api/oauthentry/youzan.items.onsale/3.0.0/get"
        self.outsale    = "https://open.youzan.com/api/oauthentry/youzan.items.inventory/3.0.0/get"
        self.delete_lis = []  # 需要删除的商品
        self.update_lis = []  # 需要更新的商品
        self.hide_lis   = []  # 需要隐藏的商品
        self.onsale_lis = []  # 在售商品
        self.outsale_lis= []  # 仓库商品
        self.basic_auth = "https://open.youzan.com/api/oauthentry/youzan.shop.basic/3.0.0/get" \
                          "?access_token="

    # 整理出删除  改动  下架的所有商品id列表
    def reorganize(self):
        for store in self.stores:
            self.sid     = store["sid"]
            update_time_start = conf.get(str(self.sid))
            end     = datetime.datetime.now().strftime("%Y-%m-%d %X")
            # endtimsp= str(int(time.time()*1000))
            conf[str(self.sid)] = end
            name    = store["name"]
            token   = store["access_token"]
            r_auth = requests.get(self.basic_auth+token)
            r_auth_json = r_auth.json()
            if r_auth_json.get("error_response"):
                print("%s店铺授权出错 可能解除授权"%(name))
                continue
            payload = {"access_token":token}
            print("--------店铺：%s (%s)-----"%(name,self.sid))
            onsale_count  = self.goods_count(self.onsale,payload)
            outsale_count = self.goods_count(self.outsale,payload)
            print("在售：%d  ，  仓库：%d"%(onsale_count,outsale_count))
            onsale_lis  = self.AllGoodsLis(self.onsale,onsale_count,payload)
            outsale_lis = self.AllGoodsLis(self.outsale,outsale_count,payload)
            cursor.execute("select item_id from third_part_wechat_praisegoods "
                           "where kdt_id=%s and is_display=1",[self.sid])
            exist_lis   = [i["item_id"] for i in cursor.fetchall()]
            if update_time_start:
                update_lis = [i["item_id"] for i in onsale_lis
                              if i["update_time"]>update_time_start]
            else:
                update_lis = [i["item_id"] for i in onsale_lis]

            onsale_lis  = [i["item_id"] for i in onsale_lis]
            outsale_lis = [i["item_id"] for i in outsale_lis]
            delete_lis  = list(set(exist_lis)-set(onsale_lis)-set(outsale_lis))
            hide_lis    = outsale_lis
            print("更新：%d ，删除：%d， 隐藏：%d"%(len(update_lis),len(delete_lis),len(hide_lis)))
            self.handerData(update_lis,onsale_lis,delete_lis,token)
            with open("sync_goods_conf.py", "w+") as f:
                st = "conf=" + str(conf)
                f.write(st)
                f.close()



    def goods_count(self,url,payload,rec=3):
        '''
        :param url:     爬取链接
        :param payload: 请求的params
        :param rec:     请求重连次数
        :return:        int
        '''
        try:
            r = requests.get(url,params=payload)
            res = r.json()
            return res["response"]["count"]
        except Exception as e:
            if rec>0:
                return self.goods_count(url,payload,rec=rec-1)
            else:
                return 0

    def AllGoodsLis(self,url,counts,payload):
        '''
        :param url:     爬取链接
        :param counts:  商品数量
        :param payload: 请求的params
        :return:        list
        '''
        rtn = []
        if counts == 0:
            return rtn
        payload["page_size"] = self.page_size
        for page_no  in range(1,counts//self.page_size+2):
            payload["page_no"] = page_no
            rtn += self.CrawlLis(url,payload)
        return rtn

    def CrawlLis(self,url,payload,rec=3):
        '''
        :param url:     爬取链接
        :param payload: 请求的params
        :param rec:     请求重连次数
        :return:        list
        '''
        try:
            r = requests.get(url,params=payload)
            res = r.json()
            data = res.get("response")["items"]
            if data:
                return [{"item_id":i["item_id"],"update_time":i["update_time"]} for i in data]
            else:
                return []
        except Exception as e:
            if rec>1:
                return self.CrawlLis(url,payload,rec=rec-1)
            else:
                return []

    def handerData(self,update_lis,onsale_lis,delete_lis,token):
        err_items = []
        params = []
        try:
            for item_id in update_lis:
                payload = {"access_token": token, "item_id": item_id}
                r_item = requests.get(self.item, params=payload)
                data = r_item.json()
                data = data.get('response')
                item = data.get('item')
                title = item.get('title')
                price = item.get('price')
                item_type = item.get('item_type')
                sold_num = item.get('sold_num')
                detail_url = item.get('detail_url')
                quantity = item.get('quantity')
                post_fee = item.get('post_fee')
                picture = item.get('pic_url')
                created_time = item.get('created')
                update_time = created_time
                alias = item.get('alias')
                post_type = item.get('post_type')
                kdt_id = item.get('kdt_id')
                is_display = 1
                cid = item.get('cid')
                item_no = item.get('item_no')
                item_tags = str(item.get('tag_ids'))[1:-1]
                ordering_type = 3
                params.append([item_id,title,price,item_type,sold_num,detail_url,
                               quantity,post_fee,picture,created_time, update_time,
                               alias,post_type,kdt_id,is_display,cid, item_no,
                               item_tags,ordering_type])
        except Exception as e:
            err_items.append(item_id)
            print(err_items)
        print("更新请求出错：%d条"%(len(err_items)))
        if params:
            sql = "replace into third_part_wechat_praisegoods" \
                  "(item_id,title,price,item_type,sold_num,detail_url," \
                  "quantity,post_fee,picture,created_time, update_time," \
                  "alias,post_type,kdt_id,is_display,cid, item_no,item_tags," \
                  "ordering_type)" \
                  "VALUES "
            sql +=",".join(["%s"]*len(params))
            cursor.execute(sql,params)
            conn.commit()
        if onsale_lis:
            cursor.execute("update third_part_wechat_praisegoods set is_display=%s "
                           "where item_id in %s and kdt_id=%s",[1,onsale_lis,self.sid])
            conn.commit()
            cursor.execute("update third_part_wechat_praisegoods set is_display=%s "
                           "where item_id not in %s and kdt_id=%s", [0,onsale_lis,self.sid])
            conn.commit()

        if  delete_lis:
            cursor.execute("select * from v_praisegoods "
                           "where kdt_id=%s and item_id not in %s",[self.sid,delete_lis])
        else:
            cursor.execute("select * from v_praisegoods "
                           "where kdt_id=%s ", [self.sid])
        res_all = cursor.fetchall()
        for i in res_all:
            i['_id'] = i['item_id']
        #  导入es
        if res_all:
            count = 20  # everytime insert es data`s count
            insert_count = int(random._ceil(len(res_all) / float(count)))
            for cut in xrange(1, insert_count + 1):
                action_lis = res_all[(cut - 1) * count:cut * count]
                ok = helpers.bulk(client, actions=action_lis, index=praise_es, doc_type="goods")

        # 删除
        # print(delete_lis)
        if delete_lis:
            s = Search(using=client,index=praise_es,doc_type="goods")\
                .filter("terms",item_id=delete_lis).delete()




if __name__ == '__main__':
    sync = SyncGoods(kdt_id=[])
    sync.reorganize()
