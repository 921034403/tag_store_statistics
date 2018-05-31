
from elasticsearch import Elasticsearch,helpers
from ms import cursor,conn


cursor.execute("select max(id) as id from third_part_wechat_mall")
res = cursor.fetchone()
max_mall =res.get("id")
es_mall_lis = [21,18,10,11,12,13,14,15,16,20,22,23,25,26,27,28,29,30,31,32,33]
es_mall_lis = es_mall_lis+[i for i in range(64,max_mall)]

host = "119.29.223.117"

client = Elasticsearch(hosts=host)
