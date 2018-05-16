# _*_ coding:utf-8 _*_
# 统计有赞的脚本

from ms import cursor,conn
from dataimport import dataimport,dataupdate1
import datetime,time
# reply_type_id = 7 公众号
# reply_type_id = 8 H5
def statistic(store_lis = None,func = None,start=None,end=None):
    if func.__name__ == 'praise_statistics1':
        print(u'统计每天互动概况')
        cursor.execute("select max(add_time_desc)  as start,kdt_id "
                       "from data_analysis_praisestatistics GROUP BY kdt_id")
        dateinfo = cursor.fetchall()

    elif  func.__name__ == 'praise_statistics2':
        print(u'统计每人互动概况')
        cursor.execute("select DATE_FORMAT(max(date_sub(add_time,INTERVAL 1 DAY)),%s) as start,kdt_id "
                       "from data_analysis_praiseuserstatistics GROUP  BY kdt_id",['%Y-%m-%d'])
        dateinfo = cursor.fetchall()
    else:
        return
    if not dateinfo:
        dateinfo = {}

    datedic = {str(i['kdt_id']): i['start'] for i in dateinfo}
    if store_lis:
        sql = "select sid,`name` from third_part_wechat_praisestore where sid in %s"
        cursor.execute(sql,[store_lis])
        store_lis = cursor.fetchall()
    else:
        sql = "select sid,`name` from third_part_wechat_praisestore "
        cursor.execute(sql)
        store_lis = cursor.fetchall()

    for store_info in store_lis:
        add_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
        store_name = store_info["name"]
        kdt_id = store_info["sid"]
        store_id = store_info["sid"]
        start = datedic.get(str(store_id))
        sql = "select id,reply_type_id,from_wechat_user_id, operation_type,from_wechat_user_id," \
              "DATE_FORMAT(operation_time,%s) as cycle,DATE_FORMAT(add_time,%s) as last_time,operation_time " \
              "from v_praise_statistic " \
              "where store_id =%s "
        params = ['%Y-%m-%d','%Y-%m-%d %H:%i:%s',store_id]
        if start:
            sql += "and DATE_FORMAT(operation_time,%s)> %s "
            params += ['%Y-%m-%d',start]
        if end:
            sql += "and DATE_FORMAT(operation_time,%s)< %s "
            params += ['%Y-%m-%d', end]

        cursor.execute(sql,params)
        res = cursor.fetchall()
        if res:
            sql = "select id,from_wechat_user_id,DATE_FORMAT(operation_time,%s) as add_time_desc," \
                  "DATE_FORMAT(operation_time,%s) as cycle from  v_praise_statistic where store_id=%s "

            params = ['%Y-%m-%d %H:%i:%s','%Y-%m-%d',kdt_id]
            if start:
                sql += "and DATE_FORMAT(operation_time,%s)>%s"
                params += ['%Y-%m-%d',start]
            if end:
                sql += "and DATE_FORMAT(operation_time,%s)<%s"
                params += ['%Y-%m-%d',end]

            cursor.execute(sql,params)
            cycle_lis = cursor.fetchall()
            func(res, add_time, store_name, kdt_id, cycle_lis,start,end)

        else:
            cycle_lis = []
            func(res, add_time, store_name, kdt_id, cycle_lis, start,end)




# 统计每天互动概况
def praise_statistics1(res,add_time,store_name,kdt_id,cycle_lis,start,end):
    print(u"统计 %s 中，店铺ID: %d 周期 ：天" % (store_name, kdt_id))
    _cycle_lis =  cycle_lis
    cycle_lis = list(set([i["cycle"] for i in cycle_lis]))
    cycle_lis.sort(key=lambda i: i, reverse=False)
    cycle_infos = {i: {"add_time_desc": i,
                                "add_time": add_time,
                                "store_name": store_name,
                                "kdt_id": kdt_id,
                                "statistics_type": 5,
                                "hf_visit_count": [],
                                "wechat_visit_count": [],
                                "hf_push_count": 0,
                                "wechat_push_count": 0,
                                "hf_click_count": 0,
                                "wechat_click_count": 0
                                } for i in cycle_lis}
    for action in res:
        reply_type_id = action["reply_type_id"]
        operation_type = action["operation_type"]
        cycle = action['cycle']
        if reply_type_id == 8:
            if operation_type == 0:
                cycle_infos[cycle]['hf_push_count'] += 1
            else:
                cycle_infos[cycle]['hf_click_count'] += 1
            cycle_infos[cycle]['hf_visit_count'].append(action['from_wechat_user_id'])
        elif reply_type_id == 7:
            if operation_type == 0:
                cycle_infos[cycle]['wechat_push_count'] += 1
            else:
                cycle_infos[cycle]['wechat_click_count'] += 1
            cycle_infos[cycle]['wechat_visit_count'].append(action['from_wechat_user_id'])
    # 结果入库前做整理
    statistics_result = []
    sql1 = "select DISTINCT(id) as id,add_time from third_part_wechat_praisechatrecord where store_id=%s "
    params1 = [kdt_id]
    if start:
        sql1 += "and DATE_FORMAT(add_time,%s)>%s "
        params1 += ['%Y-%m-%d', start]
    if end:
        sql1 += "and DATE_FORMAT(add_time,%s)<%s "
        params1 += ['%Y-%m-%d', end]
    cursor.execute(sql1, params1)
    ids1 = set([i['id'] for i in cursor.fetchall()])
    ids2 = set([i['id'] for i in _cycle_lis])
    ids = list(ids1 - ids2)
    if ids:
        sql = "select reply_type_id,from_wechat_user_id,DATE_FORMAT(add_time,%s) as add_time_desc,add_time  " \
              "from third_part_wechat_praisechatrecord " \
              "where id in %s "
        params = ['%Y-%m-%d', ids]
        cursor.execute(sql,params)
        res = cursor.fetchall()
        for action in res:
            cycle = action['add_time_desc']
            reply_type_id = action['reply_type_id']
            if cycle_infos.get(cycle):
                if reply_type_id == 8:
                    cycle_infos[cycle]['hf_visit_count'].append(action['from_wechat_user_id'])
                elif reply_type_id == 7:
                    cycle_infos[cycle]['wechat_visit_count'].append(action['from_wechat_user_id'])

    for cycle, cycle_info in cycle_infos.items():
        cycle_info['wechat_visit_count'] = len(set(cycle_info['wechat_visit_count']))
        cycle_info['hf_visit_count'] = len(set(cycle_info['hf_visit_count']))

    for cycle, cycle_info in cycle_infos.items():
        statistics_result.append(cycle_info)

    statistics_result.sort(key=lambda i: i["add_time_desc"], reverse=False)
    for cycle_info in statistics_result:
        # print(cycle_info)
        dataimport(cycle_info, "data_analysis_praisestatistics")


# 统计每人互动概况
def praise_statistics2(res,add_time,store_name,kdt_id,cycle_lis,start,end):
    print(u"统计 %s 中，店铺ID: %d 周期 ：天" % (store_name, kdt_id))
    cycle_dic = {}
    for info in cycle_lis:
        if cycle_dic.get(str(info["from_wechat_user_id"])):
            cycle_dic[str(info["from_wechat_user_id"])]['add_time_desc'] = info["add_time_desc"]
        else:
            cycle_dic[str(info["from_wechat_user_id"])] = {"add_time_desc":info["add_time_desc"],
                                                      "from_wechat_user_id":info["from_wechat_user_id"]}

    cycle_infos = {k: {"from_wechat_user_id":i["from_wechat_user_id"],
                                "add_time_desc": i['add_time_desc'],# 最后一次聊天的时间
                                "add_time": add_time,
                                "store_name": store_name,
                                "kdt_id": kdt_id,
                                "statistics_type": 5,
                                "wechat_msg_count":[],
                                "hf_msg_count": [],
                                "wechat_click_count":0,
                                "hf_click_count":0
                                } for k,i in cycle_dic.items()}
    for action in res:
        from_wechat_user_id = str(action["from_wechat_user_id"])
        reply_type_id = action["reply_type_id"]
        operation_type = action["operation_type"]
        last_time = action['last_time']
        if last_time>cycle_infos[from_wechat_user_id]['add_time_desc']:
            cycle_infos[from_wechat_user_id]['add_time_desc'] = last_time
        id = action["id"]
        if reply_type_id == 8:
            if operation_type>0:
                cycle_infos[from_wechat_user_id]["hf_click_count"] += 1
            cycle_infos[from_wechat_user_id]["hf_msg_count"].append(id)
        elif reply_type_id == 7:
            if operation_type > 0:
                cycle_infos[from_wechat_user_id]["wechat_click_count"] += 1
            cycle_infos[from_wechat_user_id]["wechat_msg_count"].append(id)
    # 结果入库前做整理
    statistics_result = []
    sql1 = "select DISTINCT(id) as id,add_time from third_part_wechat_praisechatrecord where store_id=%s "
    params1 = [kdt_id]
    if start:
        sql1 += "and DATE_FORMAT(add_time,%s)>%s "
        params1 += ['%Y-%m-%d',start]
    if end:
        sql1 += "and DATE_FORMAT(add_time,%s)<%s "
        params1 += ['%Y-%m-%d',end]
    cursor.execute(sql1,params1)
    ids1 = set([i['id'] for i in cursor.fetchall()])
    ids2 = set([i['id'] for i in cycle_lis])
    ids = list(ids1-ids2)
    if ids:
        sql = "select id,reply_type_id,from_wechat_user_id,DATE_FORMAT(add_time,%s) as last_time,add_time  " \
              "from third_part_wechat_praisechatrecord " \
              "where id in %s "
        params = ['%Y-%m-%d %H:%i:%s',ids]
        cursor.execute(sql, params)
        res = cursor.fetchall()
        for action in res:
            from_wechat_user_id = str(action["from_wechat_user_id"])
            reply_type_id = action["reply_type_id"]
            last_time = action['last_time']
            id = action['id']
            if cycle_infos.get(from_wechat_user_id):
                if reply_type_id == 8:
                    cycle_infos[from_wechat_user_id]["hf_msg_count"].append(id)
                elif reply_type_id == 7:
                    cycle_infos[from_wechat_user_id]["wechat_msg_count"].append(id)
                if last_time>cycle_infos[from_wechat_user_id]["add_time_desc"]:
                    cycle_infos[from_wechat_user_id]["add_time_desc"] = last_time
            else:
                cycle_infos[from_wechat_user_id] = {"from_wechat_user_id":action["from_wechat_user_id"],
                                    "add_time_desc": last_time,
                                    "add_time": add_time,
                                    "store_name": store_name,
                                    "kdt_id": kdt_id,
                                    "statistics_type": 5,
                                    "wechat_msg_count":[],
                                    "hf_msg_count": [],
                                    "wechat_click_count":0,
                                    "hf_click_count":0
                                    }
                if reply_type_id == 8:
                    cycle_infos[from_wechat_user_id]["hf_msg_count"].append(id)
                elif reply_type_id == 7:
                    cycle_infos[from_wechat_user_id]["wechat_msg_count"].append(id)
    for cycle, cycle_info in cycle_infos.items():
        statistics_result.append(cycle_info)
    for cycle, cycle_info in cycle_infos.items():
        cycle_info['hf_msg_count'] = len(set(cycle_info['hf_msg_count']))
        cycle_info['wechat_msg_count'] = len(set(cycle_info['wechat_msg_count']))
    statistics_result.sort(key=lambda i: i["add_time_desc"], reverse=False)
    for cycle_info in statistics_result:
        # print(cycle_info)
        dataupdate1(cycle_info, "data_analysis_praiseuserstatistics")


if __name__ == '__main__':
    start1 = None
    start2 = None
    store_lis = []
    end = datetime.datetime.now().strftime("%Y-%m-%d")
    statistic(store_lis=store_lis,func=praise_statistics1,start=start1,end=end)
    statistic(store_lis=store_lis, func=praise_statistics2, start=start2, end=end)
