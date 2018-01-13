# _*_ coding:utf-8 _*_
from ms import cursor,conn,cursor1,conn1

# 商户分析
def store_analysis(cycle='天',at_id=[2,3,4,5,6],mall_id=None,one_cycle=None):
    # cycle:统计周期
    # mall_id:商城id
    # at_id:素材类型
    # one_cycle:周期中的某一个（如：月为周期  one_cycle选择2017年12月 即取值为'2017-12-11'）
    # 默认去掉公众号文章素材(at_id=1)
    statistics_dic = dict((
        (1, '总计'),
        (2, '年'),
        (3, '月'),
        (4, '周'),
        (5, '天'),
    ))
    date_format_dic = {
        '年': '%Y',
        '月': '%Y-%m',
        '周': '%x-%v',
        '天': '%Y-%m-%d'
    }
    statistics_dic =  {value:key for key,value in statistics_dic.items()}
    fromat_style = date_format_dic.get(cycle)
    statistics_type = statistics_dic.get(cycle)
    sql1 = 'select id,chs_name,eng_name from third_part_wechat_store '
    if mall_id:
        sql1 += 'where mall_id = %s '
        sql1 = cursor.mogrify(sql1,[mall_id])
    cursor.execute(sql1)
    res1 = cursor.fetchall()
    stores = [i['id'] for i in res1]
    dic1 ={}
    for store_info in res1:
        if store_info['chs_name']:
            dic1[store_info['id']] = store_info['chs_name']
        elif store_info['eng_name']:
            dic1[store_info['id']] = store_info['eng_name']
        else:
            pass

    sql2 = 'select id,store_id from third_part_wechat_articleinformation '
    _sql2 = []
    params2 = []
    if mall_id:
        _sql2.append('store_id in %s')
        params2.append(stores)
    if at_id:
        _sql2.append('at_id in %s')
        params2.append(at_id)
    if params2:
        sql2 += 'where ' + ' and '.join(_sql2)
        print(sql2)
        sql2 = cursor.mogrify(sql2,params2)
    cursor.execute(sql2)
    res2 = cursor.fetchall()
    articles = [i['id'] for i in res2]
    dic2 = {}
    for article_info in res2:
        dic2[article_info['id']] = article_info['store_id']
    sql3 = 'select operation_type,date_format(operation_time,%s) as cycle,' \
           'chat_record_id from third_part_wechat_chatrecordarticle ' \
           'where article_id=%s '
    param3_end = []
    if one_cycle:
        sql3 += 'and date_format(operation_time,%s)=date_format(%s,%s)'
        param3_end = [fromat_style,one_cycle,fromat_style]
    for article_id in articles:
        store_name = dic1.get(dic2.get(article_id))
        analysis_one_set(sql3,article_id,fromat_style,param3_end,store_name)


def analysis_one_set(sql3,article_id,fromat_style,param3_end,store_name):
    params3 = [fromat_style, article_id] + param3_end
    try:
        cursor.execute(sql3,params3)
        res = cursor.fetchall()
    except  Exception as e:
        print(e)

    if res:
        cycle_lis = list(set([i['cycle'] for i in res]))
        cycle_lis.sort(key=lambda i: int(''.join(i.split('-'))), reverse=False)
        for cycle in cycle_lis:
            chat_records = set([i['chat_record_id'] for i in res if i['cycle']==cycle])
            ask_count = len(chat_records)
            click_count = len(set([i['chat_record_id'] for i in res if i['operation_type']==1 and i['cycle']==cycle]))
            try:
                sql = 'select count(distinct(from_username)) as counts from third_part_wechat_chatrecord where id in %s'
                cursor.execute(sql,[chat_records])
            except Exception as e:
                print(e)
                # print(cursor.mogrify(sql,[chat_records]))
                # print(article_id)
            ask_count_person = cursor.fetchone().get('counts')
            print('%s  %s  %d  %d  %d '%(store_name,cycle,ask_count,click_count,ask_count_person))

if __name__ == '__main__':
    store_analysis(cycle='月', at_id=[2, 3, 4, 5, 6], mall_id=1, one_cycle='2017-12-01')


