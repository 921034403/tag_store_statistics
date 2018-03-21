# _*_ coding:utf-8 _*_
from es_config import es_mall_lis,host
from ms import cursor,conn
from elasticsearch import Elasticsearch,helpers
from elasticsearch_dsl import Search
import random,datetime,json

def django_aip_es_v4(es_mall_lis):
    client = Elasticsearch(hosts=host)
    begin_date = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d")
    sql = "select id,chs_name from third_part_wechat_mall"
    cursor.execute(sql)
    mallinfo = cursor.fetchall()
    mall_dic = { i["id"]:i["chs_name"] for i in mallinfo}
    for malls_id in es_mall_lis:
        malls = mall_dic.get(malls_id)
        s = Search(using=client, index="django_aip_es_v4")\
            .filter("term",malls_id=21)\
            .sort("-article_id")[:1]
        response = s.execute()
        if response:
            max_article_id = response[0]["article_id"]
        else:
            max_article_id =0

        print("in  es  max article_id  is %d"%(max_article_id))
        sql = "select * from third_part_wechat_articleinformation where id>%s and mall_id=%s"
        cursor.execute(sql,[max_article_id,malls_id])
        res = cursor.fetchall()

        if res:
            count = 20  # everytime insert es data`s count
            insert_count = int(random._ceil(len(res) / float(count)))
            for cut in xrange(1,insert_count+1):
                action_lis = []
                datas = res[(cut-1)*count:cut*count]
                for action in datas:
                    sql = "select tag_name,chs_name,eng_name from v_article_tag_store where articleinformation_id=%s"
                    cursor.execute(sql,[action["id"]])
                    res = cursor.fetchall()
                    if res:
                        stores = res[0].get("chs_name") or res[0].get("eng_name")
                        tags = [i["tag_name"] for i in res]
                    else:
                        tags = []
                        sql = "select chs_name,eng_name from third_part_wechat_store where store_id =%s"
                        cursor.execute(sql,[action["store_id"]])
                        res = cursor.fetchone()
                        stores = res.get("chs_name") or res.get("eng_name")

                    action_lis.append({
                        "article_id":action["id"],
                        "title":action["title"],
                        "raw_content":action["raw_content"],
                        "thumb_url":action["thumb_url"],
                        "url":action["url"],
                        "tags":tags,
                        "send_date":action["send_date"].strftime("%Y-%m-%d"),
                        "begin_date":begin_date,
                        "malls":malls,
                        "malls_id":malls_id,
                        "stores":stores,
                        "stores_id":action["store_id"],
                        "article_type":action["at_id"]
                    })
                    # 批量导入es
                    ok,err = helpers.bulk(client, actions=action_lis, index="django_aip_es_v4", doc_type="article")
                    print(ok,err)

if __name__ == "__main__":
    django_aip_es_v4(es_mall_lis)






