
# _*_ coding:utf-8 _*_
import json,random
from ms import corpus_es,cursor,conn
from es_config import client
from elasticsearch import helpers
from elasticsearch_dsl import Search,Q

def corpus():
    # 先清空
    s = Search(using=client, index=corpus_es, doc_type="corpus") \
        .query(
        Q({'match_all': {}})
    ).delete()

    sql = "select DISTINCT(user_id) as user_id from online_bussiness_corpusbatch"
    cursor.execute(sql)
    users = cursor.fetchall()
    for user in users:
        user_id =user["user_id"]
        cursor.execute("select mall_id from users_userprofile where id = %s",[user_id])
        mallinfo = cursor.fetchone() or {}
        mall_id = mallinfo.get("mall_id")
        cursor.execute("select sid from third_part_wechat_praisestore where mall_id = %s",[mall_id])
        storeinfo = cursor.fetchone() or {}
        kdt_id = storeinfo.get("sid")
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
                    if kdt_id:
                        i["kdt_id"] = kdt_id
                    i["_id"] = i["corpusobject_id"]
                    i["question_list"] = json.loads(i["question_list"])
                    i["text_anwser"] = json.loads(i["answer_list"])['text_anwser']
                    i["praisegoods_answer"] = json.loads(i["answer_list"])['praisegoods_answer']
                    del i["answer_list"]
                ok, err = helpers.bulk(client, actions=action_lis, index=corpus_es, doc_type="corpus")
                print(ok,err)

if __name__ == "__main__":
    # 词库导入
    corpus()