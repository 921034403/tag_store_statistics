
from elasticsearch import Elasticsearch,helpers
from elasticsearch_dsl import Search

es_mall_lis = [21,18,10,11,12,13,14,15,16,20,22,23,25]
host = "119.29.223.117"

client = Elasticsearch(hosts=host)