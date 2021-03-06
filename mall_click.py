# _*_ coding:utf-8 _*_
from ms import cursor,conn
from datetime import datetime
from dataimport import dataimport
from tool import getCurDay,getFirstDayOfCurWeek,getFirstDayOfCurMonth,getFirstDayOfCurYear,getEveryCycleLastDay,isFullCycle

# 商城分析
def mall_analysis(cycle='天',at_id=None,mall_id=None,start_date = None,end_date=None):
    # cycle:统计周期
    # mall_id:商城id
    # at_id:素材类型
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
    sql = "SELECT count(*) as counts FROM django_aip.third_part_wechat_articleinformation where (at_id=2 or url<>'') and store_id in %s "
    sql1 = 'select id,chs_name,eng_name,mall_id from third_part_wechat_store '
    if mall_id:
        sql1 += 'where mall_id = %s '
        sql1 = cursor.mogrify(sql1,[mall_id])
    cursor.execute(sql1)
    res1 = cursor.fetchall()
    store_ids = [i['id'] for i in res1]
    sql2 = 'select id from third_part_wechat_articleinformation '
    _sql2 = []
    params2 = []
    if store_ids:
        cursor.execute(sql,[store_ids])
        # 可点击素材数量
        article_count = cursor.fetchone().get('counts')
        _sql2.append('store_id in %s')
        params2.append(store_ids)
        if at_id:
            _sql2.append('at_id in %s')
            params2.append(at_id)
        sql2 += 'where ' + ' and '.join(_sql2)
        cursor.execute(sql2,params2)
        res2 = cursor.fetchall()
        article_ids = [i['id'] for i in res2]
        sql3 = "select operation_type,date_format(operation_time,%s) as cycle," \
               "chat_record_id from third_part_wechat_chatrecordarticle " \
               "where article_id in %s "
        param3 = [fromat_style,article_ids]
        if start_date:
            sql3 += 'and operation_time > %s '
            param3.append(start_date)
        if end_date:
            sql3 += 'and operation_time < %s '
            param3.append(end_date)
        sql3 +='order by operation_time '
        cursor.execute(sql3,param3)
        res3 = cursor.fetchall()
        if res3:
            analysis_one_set(res3,mall_id,article_count,statistics_type)


def analysis_one_set(res,mall_id,article_count,statistics_type):
    cycle_lis = list(set([i['cycle'] for i in res]))
    cycle_lis.sort(key=lambda i: int(''.join(i.split('-'))), reverse=False)
    for add_time_desc in cycle_lis:
        # chat_records_repeat = [i['chat_record_id'] for i in res if i['operation_type']==0 and i['cycle']==add_time_desc]
        chat_records_click_repeat = [i['chat_record_id'] for i in res if i['operation_type']==1 and i['cycle']==add_time_desc]
        # 不去重的点击数
        click_num = len(chat_records_click_repeat)
        add_time = getEveryCycleLastDay(add_time_desc,statistics_type)
        click_num_avg = round(click_num/float(article_count),2)
        data_dic = {'mall_id':mall_id,
                    'statistics_type':statistics_type,
                    'click_num':click_num,
                    'add_time':add_time,
                    'click_num_avg':click_num_avg}
        dataimport(data_dic,'data_analysis_mallclickcumulate')
        # print(u'商城id：%d ，统计周期类型：%d ，点击次数：%d ，素材平均点击次数：%d ，周期最后一天的日期：%s'
        #       %(mall_id, statistics_type, click_num,click_num_avg,add_time))

def main():
    print('商城分析  data_analysis_mallclickcumulate')
    sql_all = 'SELECT statistics_type, max(add_time) as last_time FROM django_aip.data_analysis_mallclickcumulate group by statistics_type'
    sql_mall = 'select id from django_aip.third_part_wechat_mall'
    cursor.execute(sql_all)
    res_alltime = cursor.fetchall()
    cursor.execute(sql_mall)
    malls = [i['id'] for i in cursor.fetchall()]
    statistics_dic = dict((
        (2, '年'),
        (3, '月'),
        (4, '周'),
        (5, '天'),
    ))
    if not res_alltime:
        res_alltime = [{'statistics_type':i,'last_time':None } for i in statistics_dic]
    exies_statistics = set([i['statistics_type'] for i in res_alltime])
    all_statistics = set([i for i in statistics_dic])
    not_exies_statistics = all_statistics - exies_statistics
    if not_exies_statistics:
        not_exies_statistics = list(not_exies_statistics)
        for i in not_exies_statistics:
            res_alltime.append({'statistics_type': i, 'last_time': None})
    for i in res_alltime:
        statistics_type = i['statistics_type']
        cycle = statistics_dic.get(statistics_type)
        start_date = i.get('last_time') or '2017-01-01'
        if statistics_type == 2:
            end_date = getFirstDayOfCurYear()
        elif statistics_type == 3:
            end_date = getFirstDayOfCurMonth()
        elif statistics_type == 4:
            end_date = getFirstDayOfCurWeek()
        elif statistics_type == 5:
            end_date = getCurDay()
        for mall_id in malls:
            flag = isFullCycle(start_date,end_date,cycle)
            if flag:
                # print('统计周期：%s  统计日期范围：%s--%s  统计商城id：%d ' % (cycle, start_date, end_date, mall_id))
                mall_analysis(cycle=cycle, at_id=None, mall_id=mall_id, start_date=start_date,end_date=end_date)


if __name__ == '__main__':
    main()