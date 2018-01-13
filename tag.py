# _*_ coding:utf-8 _*_
from ms import cursor, conn,cursor1,conn1

# 标签分析


def tag_analysis(cycle='天',at_id=[2,3,4,5,6],mall_id=None,one_cycle=None):
    '''
    SELECT `b`.`name` as `name`,`b`.`english_name` as `english_name`,concat(`english_name`,`name`) as `catname`,count(concat(`english_name`,`name`)) as counts FROM ibi.t_ibi_search_log as a inner join ibi.t_ibi_store as b on (a.store_id=b.id and date_format(a.create_time,'%Y %m')=date_format('2017-12-11','%Y %m')) group by  catname order by counts desc;
    '''
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

    sql5 = "select operation_type,DATE_FORMAT(operation_time,%s) as cycle," \
           "unix_timestamp(operation_time) as time_zone " \
           "from third_part_wechat_chatrecordarticle " \
           "where chat_record_id in %s and article_id in %s "
    params5 = [fromat_style]
    end_params5 = []
    if one_cycle:
        sql5 += 'and DATE_FORMAT(operation_time,%s)= DATE_FORMAT(%s,%s) '
        end_params5 += [fromat_style,one_cycle,fromat_style]
    sql5 += 'ORDER BY cycle '
    _analysis_dic = {}
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
            analysis_dic = analysis_one_set(res,tag_name,tag_type,mall_id,statistics_type)
    #         _analysis_dic.update(analysis_dic)
    # marry(_analysis_dic)



def analysis_one_set(res,tag_name,tag_type,mall_id,statistics_type):
    # print('----------------------------------->')
    cycle_lis = list(set([i['cycle'] for i in res]))
    cycle_lis.sort(key=lambda i:int(''.join(i.split('-'))),reverse=False)
    analysis_dic = {}
    for cycle in cycle_lis:
        ask = {}
        click = {}
        ask_count = 0
        click_count = 0
        for one_set in res:
            if one_set['cycle'] == cycle:
                if ask.get(str(one_set.get('time_zone'))):
                    pass
                else:
                    ask[str(one_set.get('time_zone'))]=1
                    ask_count += 1
                if one_set['operation_type']==1:
                    if click.get(str(one_set.get('time_zone'))):
                        pass
                    else:
                        click[str(one_set.get('time_zone'))]=1
                        click_count += 1
        if tag_type:
            # analysis_dic[tag_name] = [ask_count,tag_type]
            print(u'周期描述:%s 商城:%d 关键词:%s 类型:%d 周期类型:%d 呼叫次数:%d 点击次数:%d 周期最后日期:%s'%(cycle,mall_id,tag_name,tag_type,statistics_type,ask_count,click_count,'1'))
        else:
            print(u'关键词:%s 没有tag_type'%(tag_name))
    # return analysis_dic


def marry(analysis_dic):
    sql = "SELECT key_type, key_name,count(key_name) as counts, " \
          "date_format(create_time,'%Y-%m') as cycle_desc " \
          "FROM ibi.t_ibi_search_log where " \
          "date_format(create_time,'%Y %m') =date_format('2017-12-11','%Y %m')  " \
          "group by key_name order by counts desc;"
    cursor1.execute(sql)
    res = cursor1.fetchall()
    dic = {}
    for i in res:
        dic[i['key_name']] = [i['counts'],i['key_type']]

    for key_name in dic:
        if analysis_dic.get(key_name):
            analysis_dic[key_name][0] += dic[key_name][0]
        else:
            analysis_dic[key_name] = dic[key_name]
    brand_lis = []
    simple_lis = []
    for tag_name,values in analysis_dic.items():
        tag_type = values[1]
        counts = values[0]
        _dic = {'tag_name': tag_name, 'tag_type': tag_type, 'counts': counts}
        if tag_type == 1:
            brand_lis.append(_dic)
        else:
            simple_lis.append(_dic)

    brand_lis.sort(key=lambda i:i['counts'],reverse=True)
    simple_lis.sort(key=lambda i:i['counts'],reverse=True)
    print('品牌关键词前20')
    for tag_info in brand_lis[:20]:
        print('%s  %d' %(tag_info['tag_name'],tag_info['counts']))

    print('普通关键词前20')
    for tag_info in simple_lis[:20]:
        print('%s  %d' % (tag_info['tag_name'], tag_info['counts']))













if __name__ == '__main__':

    tag_analysis(cycle='年',at_id=[2,3,4,5,6],mall_id=1,one_cycle=None)