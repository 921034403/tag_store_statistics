# _*_ coding:utf-8 _*_
from ms import cursor,conn
from datetime import datetime
from dataimport import dataimport,dataupdate
from tool import getCurDay,getFirstDayOfCurWeek,getFirstDayOfCurMonth,getFirstDayOfCurYear,getEveryCycleLastDay,isFullCycle

# 互动量分析(商城为单位)
def streammsg_analysis(cycle=None,at_id=None,mall_id=None,start_date =None,end_date=None,wechat_account_id=None,to_username=None):
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
    sql = 'select id,from_username,ask_type_id,reply_type_id,DATE_FORMAT(add_time,%s) as cycle from django_aip.third_part_wechat_chatrecord ' \
          'where to_username=%s '
    params = [fromat_style, to_username]
    if start_date:
        sql += 'and add_time>%s '
        params.append(start_date)
    if end_date:
        sql += 'and add_time<%s '
        params.append(end_date)
    sql += 'order by add_time '
    cursor.execute(sql,params)
    res = cursor.fetchall()
    if res:
        analysis_one_set(res,mall_id,statistics_type,wechat_account_id,to_username)


def analysis_one_set(res,mall_id,statistics_type,wechat_account_id,to_username):
    cycle_lis = list(set([i['cycle'] for i in res]))
    cycle_lis.sort(key=lambda i: int(''.join(i.split('-'))), reverse=False)
    for add_time_desc in cycle_lis:
        ref_date = getEveryCycleLastDay(add_time_desc, statistics_type)
        all_msg_user = 0
        all_msg_count = 0
        all_msg_total_count = 0
        for ask_type_id in [1,2,3]:
            # 图灵类型统计
            webot_counts = len([i['id'] for i in res if i['cycle'] ==add_time_desc and i['reply_type_id']==5 ])
            chatrecord_ids = [i['id'] for i in res if i['cycle'] ==add_time_desc and i['ask_type_id']==ask_type_id ]
            msg_user_repeat = [i['from_username'] for i in res if i['cycle'] ==add_time_desc and i['ask_type_id']==ask_type_id ]
            msg_user =len(set(msg_user_repeat))
            if msg_user:
                msg_count = len(msg_user_repeat)
                msg_count_avg = round(msg_count/float(msg_user),2)
                msg_total_count1 = msg_count
                sql = 'select count(*) as `count` from django_aip.third_part_wechat_chatrecordarticle ' \
                      'where operation_type=0 and chat_record_id in %s '
                cursor.execute(sql,[chatrecord_ids])
                msg_total_count2 = cursor.fetchone().get('count')
                msg_total_count = msg_total_count1 + msg_total_count2+webot_counts
                msg_total_avg = round(msg_total_count/float(msg_user),2)
                data_dic = {
                    'msg_user':msg_user,
                    'msg_count':msg_count,
                    'msg_type':ask_type_id,
                    'ref_date':ref_date,
                    'wechat_account_id':wechat_account_id,
                    'msg_total_count':msg_total_count,
                    'msg_count_avg':msg_count_avg,
                    'msg_total_avg':msg_total_avg
                }
                dataupdate(data_dic,'data_analysis_upstreammsgday')
                # print(data_dic['ref_date'], data_dic['wechat_account_id'], data_dic['msg_user'],
                #       data_dic['msg_count'], data_dic['msg_total_count'],
                #       data_dic['msg_count_avg'], data_dic['msg_total_avg'], data_dic['msg_type'])
                all_msg_user += msg_user
                all_msg_count += msg_count
                all_msg_total_count += msg_total_count
        all_msg_count_avg = round(all_msg_count/float(all_msg_user),2)
        all_msg_total_avg = round(all_msg_total_count / float(all_msg_user), 2)
        data_dic = {
            'msg_user': all_msg_user,
            'msg_count': all_msg_count,
            'msg_type': 6,
            'ref_date': ref_date,
            'wechat_account_id': wechat_account_id,
            'msg_total_count': all_msg_total_count,
            'msg_count_avg': all_msg_count_avg,
            'msg_total_avg': all_msg_total_avg
        }
        dataupdate(data_dic, 'data_analysis_upstreammsgday')



def main():
    print('互动量分析(商城为单位) data_analysis_upstreammsgday')
    sql_all = 'SELECT  wechat_account_id,max(ref_date) as last_time FROM django_aip.data_analysis_upstreammsgday group by wechat_account_id'
    sql_mall = "select mall_id,id as wechat_account_id,user_name from django_aip.third_part_wechat_wechataccount where mall_id<>''"
    cursor.execute(sql_all)
    res_alltime = cursor.fetchall()
    cursor.execute(sql_mall)
    res_malls = cursor.fetchall()
    wechat_accounts = [i['wechat_account_id'] for i in res_malls]
    mall_wechat_dic = {i['wechat_account_id']:i['mall_id'] for i in res_malls}
    mall_username_dic = {i['wechat_account_id']:i['user_name'] for i in res_malls}
    statistics_dic = dict((
        # (2, '年'),
        # (3, '月'),
        # (4, '周'),
        (5, '天'),
    ))
    if not res_alltime:
        res_alltime = [{'statistics_type':5,'wechat_account_id':i,'last_time':None} for i in wechat_accounts]
    else:
        exexits_wechat_accounts = set([i['wechat_account_id'] for i in res_alltime])
        not_exexits_wechat_accounts = set(wechat_accounts) - exexits_wechat_accounts
        not_exexits_wechat_accounts = list(not_exexits_wechat_accounts).sort(key=lambda i:i,reverse=False)
        if not_exexits_wechat_accounts:
            for i in not_exexits_wechat_accounts:
                res_alltime.append({'statistics_type':5,'wechat_account_id':i,'last_time':None})
    for i in res_alltime:
        i.update({'statistics_type':5})
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
        wechat_account_id = i['wechat_account_id']
        mall_id = mall_wechat_dic.get(wechat_account_id)
        to_username = mall_username_dic.get(wechat_account_id)
        flag = isFullCycle(start_date,end_date,cycle)
        if flag:
            # print('统计周期：%s  统计日期范围：%s--%s  统计商城id：%d ' % (cycle, start_date, end_date, mall_id))
            streammsg_analysis(cycle=cycle, at_id=None, mall_id=mall_id, start_date=start_date,end_date=end_date,wechat_account_id=wechat_account_id,to_username=to_username)


if __name__ == '__main__':
    main()