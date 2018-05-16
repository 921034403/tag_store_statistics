# _*_ coding:utf-8 _*_
from es_config import es_mall_lis,client,host
from ms import cursor,conn,corpus_es,praise_es
from elasticsearch import Elasticsearch,helpers
from elasticsearch_dsl import Search,Q
import random,datetime,json



# 批量导入本地宝  微时光新文章数据
def django_aip_es(es_mall_lis):
    article_id_end = None
    begin_date = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d")
    sql = "select id,chs_name from third_part_wechat_mall"
    cursor.execute(sql)
    mallinfo = cursor.fetchall()
    mall_dic = {i["id"]:i["chs_name"] for i in mallinfo}
    for malls_id in es_mall_lis:
        malls = mall_dic.get(malls_id)
        s = Search(using=client, index="django_aip_es")\
            .filter("term",malls_id=malls_id)\
            .sort("-article_id")[:1]
        response = s.execute()
        if response:
            max_article_id = response[0]["article_id"]
        else:
            max_article_id =0

        print("in  es  max article_id  is %d"%(max_article_id))
        sql = "select * from third_part_wechat_articleinformation where id>%s and mall_id=%s and at_id in %s order by id"
        cursor.execute(sql,[max_article_id,malls_id,[1,4]])
        res_all = cursor.fetchall()
        print(u"%d   %s %d条新数据"%(malls_id,malls,len(res_all)))
        if res_all:
            count = 20  # everytime insert es data`s count
            insert_count = int(random._ceil(len(res_all) / float(count)))
            for cut in xrange(1,insert_count+1):
                action_lis = []
                datas = res_all[(cut-1)*count:cut*count]
                for action in datas:
                    article_id_end = action["id"]
                    sql = "select tag_name,chs_name,eng_name from v_article_tag_store where articleinformation_id=%s"
                    cursor.execute(sql,[action["id"]])
                    res = cursor.fetchall()
                    if res:
                        stores = res[0].get("chs_name") or res[0].get("eng_name")
                        tags = [i["tag_name"] for i in res]
                    else:
                        tags = []
                        sql = "select chs_name,eng_name from third_part_wechat_store where id= %s"
                        cursor.execute(sql,[action["store_id"]])
                        res = cursor.fetchone()
                        stores = res.get("chs_name") or res.get("eng_name")

                    action_lis.append({
                        "article_id":action["id"],
                        "title":action["title"],
                        # "raw_content":action["raw_content"],
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

                ok,err = helpers.bulk(client, actions=action_lis, index="django_aip_es", doc_type="article")
                print(ok,err)
        print(max_article_id,article_id_end)
        # 清除raw_content
        update_at_sql = "update third_part_wechat_articleinformation set raw_content=%s where mall_id=%s and id>%s and id<=%s "
        cursor.execute(update_at_sql,[None,malls_id,max_article_id,article_id_end])
        conn.commit()


# 批量导入有赞数据
def django_aip_es_praise():
    sql = "select * from v_praisegoods"
    cursor.execute(sql)
    res_all = cursor.fetchall()
    for i in res_all:
        i['_id'] = i['item_id']
        # print(i)
    if res_all:
        count = 20  # everytime insert es data`s count
        insert_count = int(random._ceil(len(res_all) / float(count)))
        for cut in xrange(1, insert_count + 1):
            action_lis = res_all[(cut - 1) * count:cut * count]
            ok = helpers.bulk(client,actions=action_lis, index=praise_es, doc_type="goods")
            print(ok)





# 更新有赞数据
def update_django_aip_es_praise():
    _id = '42'
    client.index('blog','doc',{'aa':1},id)


def empty_praise(lis=None):
    if type(lis) == list and lis:
        for i in lis:
            client = Elasticsearch(hosts=host)
            s = Search(using=client, index=i, doc_type="goods") \
                .query(
                Q({'match_all': {}})
            ).delete()
    else:
        print("no actions")

def delete_praise(lis=None):
    if type(lis) ==list and lis:
        for i in lis:
            client = Elasticsearch(hosts=host)
            client.indices.delete(index=i, ignore=[400, 404])
    else:
        print("no actions")


def search():
    s = Search(using=client, index=praise_es, doc_type="goods") \
            .query(
        Q("bool", must=[Q("multi_match", query='吃的', fields=['title'])])
            ) \
            .query(
        Q("bool", must=[Q("multi_match", query="100889764", fields=['item_tags'])])
    ) \
            .filter("term", malls_id=24) \
            .source(
        includes=['id', 'sold_num', 'goods_name', 'img_url', 'title', 'price', 'detail_url', 'name', 'created_time']) \
            .sort('_score')[:2]
    r = s.execute()
    print(r)


def corpus():
    sql = "select DISTINCT(user_id) as user_id from online_bussiness_corpusbatch"
    cursor.execute(sql)
    users = cursor.fetchall()
    for user in users:
        user_id =user["user_id"]
        cursor.execute("select mall_id from users_userprofile where id = %s",[user_id])
        mallinfo = cursor.fetchone() or {}
        mall_id = mallinfo.get("mall_id")
        sql = "SELECT * FROM v_corpus where user_id =%s "
        cursor.execute(sql,[user_id])
        data = cursor.fetchall()
        if data:
            count = 20  # everytime insert es data`s count
            insert_count = int(random._ceil(len(data) / float(count)))
            for cut in xrange(1, insert_count + 1):
                action_lis = data[(cut - 1) * count:cut * count]
                for i in action_lis:
                    if mall_id:
                        i["mall_id"] = mall_id
                    i["_id"] = i["corpusobject_id"]
                    i["question_list"] = json.loads(i["question_list"])
                    i["text_anwser"] = json.loads(i["answer_list"])['text_anwser']
                    i["praisegoods_answer"] = json.loads(i["answer_list"])['praisegoods_answer']
                    del i["answer_list"]
            ok, err = helpers.bulk(client, actions=action_lis, index=corpus_es, doc_type="corpus")
            print(ok,err)






if __name__ == "__main__":
    #todo  有赞数据导入
    # django_aip_es_praise()


    # todo 公众号文章导入
    django_aip_es(es_mall_lis)


    # todo 更新测试
    # update_django_aip_es_praise()


    # todo 清空index
    # delete_praise([corpus_es])



    # todo 删除index
    # delete_praise([corpus_es])







