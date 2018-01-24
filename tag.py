# _*_ coding:utf-8 _*_
from ms import cursor, conn
from dataimport import dataimport
from tool import getCurDay,getFirstDayOfCurWeek,getFirstDayOfCurMonth,getFirstDayOfCurYear,getEveryCycleLastDay
# 标签分析


def tag_analysis(cycle=None,at_id=None,mall_id=None, start_date=None, end_date=None):
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
    statistics_dic = {value: key for key, value in statistics_dic.items()}
    fromat_style = date_format_dic.get(cycle)
    statistics_type = statistics_dic.get(cycle)
    sql1 = 'select distinct(tag_id) as tag_id from third_part_wechat_chatrecordtag'
    sql3 = 'select id from third_part_wechat_articleinformation '
    params3 =[]
    if at_id:
        sql3 += 'where at_id in %s '
        params3.append(at_id)

    if mall_id:
        sql2 = 'select id from third_part_wechat_store where mall_id= %s'
        cursor.execute(sql2,[mall_id])
        stores =[i['id'] for i in cursor.fetchall()]
        if stores:
            params3.append(stores)
            if at_id:
                sql3 += 'and store_id in %s'
            else:
                sql3 += 'where store_id in %s'
    cursor.execute(sql1)
    tags = cursor.fetchall()
    if params3:
       cursor.execute(sql3,params3)
    else:
        cursor.execute(sql3)
    artcle_lis = [i['id'] for i in cursor.fetchall()]
    if artcle_lis:
        flag_sql = 'select * from third_part_wechat_chatrecordarticle where article_id in %s '
        cursor.execute(flag_sql,[artcle_lis])
        flag = cursor.fetchall()
        if flag:
            sql5 = "select chat_record_id,operation_type,DATE_FORMAT(operation_time,%s) as cycle," \
               "unix_timestamp(operation_time) as time_zone " \
               "from third_part_wechat_chatrecordarticle " \
               "where chat_record_id in %s and article_id in %s "
            params5 = [fromat_style]
            end_params5 = []
            if start_date:
                sql5 += 'and operation_time > %s '
                end_params5.append(start_date)
            if end_date:
                sql5 += 'and operation_time < %s '
                end_params5.append(end_date)
            sql5 += 'ORDER BY cycle '
            for i in tags:
                tag_id = i['tag_id']
                sql4 = 'select distinct(chat_record_id) as chat_record_id from third_part_wechat_chatrecordtag where tag_id=%s'
                cursor.execute(sql4,[tag_id])
                chat_records = [i['chat_record_id'] for i in cursor.fetchall()]
                mid_params5 = [chat_records, artcle_lis]
                _params5 = params5 +mid_params5 + end_params5
                cursor.execute(sql5,_params5)
                res = cursor.fetchall()
                if res:
                    sql6 ='select tag_name,tag_type from third_part_wechat_tags where id =%s'
                    cursor.execute(sql6,[tag_id])
                    tag_info = cursor.fetchone()
                    tag_name = tag_info.get('tag_name')
                    tag_type = tag_info.get('tag_type')
                    mall_id = mall_id
                    analysis_one_set(res,tag_name,tag_type,mall_id,statistics_type)



def analysis_one_set(res,tag_name,tag_type,mall_id,statistics_type):
    cycle_lis = list(set([i['cycle'] for i in res]))
    cycle_lis.sort(key=lambda i:int(''.join(i.split('-'))),reverse=False)
    for add_time_desc in cycle_lis:
        chat_record_lis = []
        click_chat_record_lis = []
        for one_set in res:
            if one_set['cycle'] == add_time_desc:
                chat_record_lis.append(one_set['chat_record_id'])
                if one_set['operation_type']==1:
                    click_chat_record_lis.append(one_set['chat_record_id'])
        chat_record_lis = list(set(chat_record_lis))
        click_chat_record_lis = list(set(click_chat_record_lis))
        ask_count = len(chat_record_lis)
        click_count = len(click_chat_record_lis)
        sql = 'select count(distinct(from_username)) as counts from third_part_wechat_chatrecord where id in %s'
        cursor.execute(sql,[chat_record_lis])
        ask_count_info = cursor.fetchone() or {}
        ask_count_person = ask_count_info.get('counts') or 0
        add_time = getEveryCycleLastDay(add_time_desc,statistics_type)
        convert_rate = round(click_count*100/float(ask_count),2)
        if not tag_type:
            tag_type =2
        data_dic = {
               'tag_name':tag_name,
               'tag_type':tag_type,
               'statistics_type':statistics_type,
               'ask_count_person':ask_count_person,
               'convert_rate':convert_rate,
               'ask_count':ask_count,
               'add_time':add_time,
               'mall_id':mall_id,
               'add_time_desc':add_time_desc}
        dataimport(data_dic,'data_analysis_keywordcumulate')

        # print(u'关键词:%s ,类型:%d ,周期类型:%d ,呼叫次数:%d 来访人数:%d 转化率：%.2f 周期最后日期:%s 商城：%s 周期描述：%s'
        #       %(tag_name,tag_type,statistics_type,ask_count,ask_count_person,convert_rate,add_time,mall_id,add_time_desc))




def main():
    sql_all = 'SELECT statistics_type, max(add_time) as last_time FROM django_aip.data_analysis_keywordcumulate group by statistics_type'
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
            res_alltime.append({'statistics_type':i,'last_time':None})
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
            if cycle== '天':
                print('统计周期：%s  统计日期范围：%s--%s  统计商城id：%d '%(cycle,start_date,end_date,mall_id))
                tag_analysis(cycle=cycle, at_id=None, mall_id=mall_id, start_date=start_date,end_date=end_date)










if __name__ == '__main__':
    main()
