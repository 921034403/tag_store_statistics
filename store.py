# _*_ coding:utf-8 _*_
from ms import cursor,conn
from datetime import datetime
from dataimport import dataimport
from tool import getCurDay,getFirstDayOfCurWeek,getFirstDayOfCurMonth,getFirstDayOfCurYear,getEveryCycleLastDay,isFullCycle

# 商户分析
def store_analysis(cycle='天',at_id=None,mall_id=None,start_date = None,end_date=None):
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
    sql1 = 'select id,chs_name,eng_name,mall_id from third_part_wechat_store '
    if mall_id:
        sql1 += 'where mall_id = %s '
        sql1 = cursor.mogrify(sql1,[mall_id])
    cursor.execute(sql1)
    res1 = cursor.fetchall()
    for store_info in res1:
        chs_name = store_info['chs_name']
        eng_name = store_info['eng_name']
        mall_id = store_info['mall_id']
        store_id = store_info['id']
        sql2 = 'select id,store_id from third_part_wechat_articleinformation '
        _sql2 = []
        params2 = []
        if store_id:
            _sql2.append('store_id = %s')
            params2.append(store_id)
        if at_id:
            _sql2.append('at_id in %s')
            params2.append(at_id)
        if params2:
            sql2 += 'where ' + ' and '.join(_sql2)
            sql2 = cursor.mogrify(sql2,params2)
        cursor.execute(sql2)
        res2 = cursor.fetchall()
        articles = [i['id'] for i in res2]
        sql3 = "select operation_type,date_format(operation_time,%s) as cycle," \
               "chat_record_id from third_part_wechat_chatrecordarticle " \
               "where article_id in %s "
        param3_end = []
        if start_date:
            sql3 += 'and operation_time > %s '
            param3_end += [start_date]
        if end_date:
            sql3 += 'and operation_time < %s '
            param3_end += [end_date]
        if chs_name:
            store_name = chs_name
        else:
            store_name = eng_name

        if articles:
            analysis_one_set(sql3,articles,fromat_style,param3_end,store_name,statistics_type,mall_id)


def analysis_one_set(sql3,article_id,fromat_style,param3_end,store_name,statistics_type,mall_id):
    params3 = [fromat_style, article_id] + param3_end
    try:
        cursor.execute(sql3,params3)
        res = cursor.fetchall()
    except  Exception as e:
        print(sql3)
        print(params3)
        print(e)

    if res:
        cycle_lis = list(set([i['cycle'] for i in res]))
        cycle_lis.sort(key=lambda i: int(''.join(i.split('-'))), reverse=False)
        for add_time_desc in cycle_lis:
            chat_records = set([i['chat_record_id'] for i in res if i['operation_type']==0 and i['cycle']==add_time_desc])
            chat_records_click = set([i['chat_record_id'] for i in res if i['operation_type']==1 and i['cycle']==add_time_desc])
            # 去重的呼出次数
            ask_count = len(chat_records)
            # 去重的点击数
            click_num = len(chat_records_click)
            try:
                sql = 'select count(distinct(from_username)) as counts from third_part_wechat_chatrecord where id in %s'
                if len(chat_records)>1:
                    cursor.execute(sql,[chat_records])
                    ask_count_person = cursor.fetchone().get('counts')
                else:
                    ask_count_person = len(chat_records)

                if len(chat_records_click)>1:
                    cursor.execute(sql, [chat_records_click])
                    click_num_person = cursor.fetchone().get('counts')
                else:
                    click_num_person = len(chat_records_click)
            except Exception as e:
                print(e)
                # print(cursor.mogrify(sql,[chat_records]))
                # print(article_id)
            add_time = getEveryCycleLastDay(add_time_desc,statistics_type)
            convert_rate = round(click_num*100/float(ask_count),2)
            data_dic = {'store_name':store_name,
                        'statistics_type':statistics_type,
                        'ask_count':ask_count,
                        'ask_count_person':ask_count_person,
                        'click_num': click_num,
                        'click_num_person': click_num_person,
                        'convert_rate':convert_rate,
                        'add_time':add_time,
                        'mall_id':mall_id,
                        'add_time_desc':add_time_desc}
            # dataimport(data_dic,'data_analysis_storecumulate')
            print(u'商户：%s ，统计周期类型：%d ，呼叫次数：%d ，点击次数：%d ，点击人数：%d ，来访人数：%d ，访问概率：%.2f ，周期最后一天的日期：%s ，商城id：%d ，周期描述：%s '
                  %(store_name, statistics_type, ask_count,click_num,click_num_person, ask_count_person, convert_rate, add_time, mall_id, add_time_desc))

def main():
    sql_all = 'SELECT statistics_type, max(add_time) as last_time FROM django_aip.data_analysis_storecumulate group by statistics_type'
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
                print('统计周期：%s  统计日期范围：%s--%s  统计商城id：%d ' % (cycle, start_date, end_date, mall_id))

                store_analysis(cycle=cycle, at_id=None, mall_id=mall_id, start_date=start_date,end_date=end_date)


if __name__ == '__main__':
    main()