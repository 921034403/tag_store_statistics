
from elasticsearch import Elasticsearch,helpers
from elasticsearch_dsl import Search

es_mall_lis = [21,18,10,11,12,13,14,15,16,20,22,23,25,26,27,28,29,30,31,32,33]
es_mall_lis = es_mall_lis+[i for i in range(64,155)]
# es_mall_lis = [25]

host = "119.29.223.117"

client = Elasticsearch(hosts=host)
praise_es = 'django_aip_es_praise_v6'