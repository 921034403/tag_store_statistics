# _*_ coding:utf-8 _*_

from es_config import host
from elasticsearch_dsl import DocType, Text, Date, Integer,Q,Keyword,analyzer,token_filter,tokenizer
from elasticsearch_dsl.connections import connections
# Define a default Elasticsearch client
connections.create_connection(hosts=[host])


pinyin_analyzer = analyzer('pinyin_analyzer',
                           tokenizer=tokenizer('my_pinyin', type='pinyin', lowercase=True)
                           )
local_dynamic_synonym_filter = token_filter(name_or_instance="local_synonym",
                                            type="dynamic_synonym",
                                            synonyms_path="synonyms.txt",
                                            interval=60)
local_synonym = token_filter(name_or_instance="local_synonym",
                             type="dynamic_synonym",
                             synonyms_path="synonyms.txt",
                             interval=30)

ik_synonym_analyzer = analyzer("remote_ik_synonym_analyzer ",
                                      tokenizer='ik_max_word',
                                      filter=[local_synonym])
ik_smart_synonym = analyzer("ik_smart_synonym", tokenizer='ik_smart', filter=[local_dynamic_synonym_filter])
ik_max_word_synonym = analyzer("ik_max_word_synonym", tokenizer='ik_max_word', filter=[local_dynamic_synonym_filter])

class Article(DocType):
    id = Integer()
    sid = Integer()
    name = Keyword()
    logo = Keyword()
    malls_id = Integer()
    malls = Keyword()
    goods_name = Text(analyzer='ik_smart',store=True)
    title = Text(analyzer='ik_smart',store=True)
    price = Integer()
    goods_type = Integer()
    goods_num =Integer()
    detail_url = Keyword()
    post_fee = Integer()
    img_url = Keyword()
    goods_no  =  Keyword()
    created_time = Date()
    update_time = Date()
    alias = Keyword()
    post_type = Integer()
    sold_num = Integer()
    class Meta:
        index = "django_aip_es_praise"
        doc_type = 'goods'

class DjangoAipCorpus_V1(DocType):
    user_id = Integer()
    mall_id = Integer()
    batch_id = Integer()
    corpusobject_id = Integer()
    kdt_id = Integer()
    batch_file = Keyword()
    question_list = Text(analyzer=ik_max_word_synonym,
                         fields={"pinyin":Text(analyzer=pinyin_analyzer)})
    text_anwser = Keyword()
    praisegoods_answer = Keyword()
    corpus_batch_name = Keyword()
    update_time = Date()
    add_time = Date()
    class Meta:
        index = 'django_aip_corpus_v1'
        doc_type = 'corpus'

    user_id = Integer()
    mall_id = Integer()
    batch_id = Integer()
    corpusobject_id = Integer()
    kdt_id = Integer()
    batch_file = Keyword()
    question_list = Text(analyzer=ik_max_word_synonym,
                         fields={"pinyin":Text(analyzer=pinyin_analyzer)})
    text_anwser = Keyword()
    praisegoods_answer = Keyword()
    corpus_batch_name = Keyword()
    update_time = Date()
    add_time = Date()
    class Meta:
        index = 'django_aip_corpus_v1'
        doc_type = 'corpus'

if __name__ == '__main__':
    DjangoAipCorpus_V1.init()