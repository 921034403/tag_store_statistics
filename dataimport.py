from ms import cursor,conn



def dataimport(data_dic,table):
    lis = [i for i in data_dic]
    key = ','.join(lis)
    _s = ','.join(['%s']*len(lis))
    values = [data_dic[i] for i in lis]
    sql = 'insert into {0}({1})VALUE({2}) '
    sql = sql.format(table,key,_s)
    cursor.execute(sql,values)
    conn.commit()


def dataupdate(data_dic,table):
    sql = 'update data_analysis_upstreammsgday set msg_total_count=%s,msg_total_avg=%s ' \
          'where ref_date=%s and wechat_account_id=%s and msg_type=%s '

    params = [data_dic['msg_total_count'] , data_dic['msg_total_avg'],
              data_dic['ref_date'], data_dic['wechat_account_id'], data_dic['msg_type']
              ]
    cursor.execute(sql,params)
    conn.commit()

def dataupdate1(data_dic,table):
    sql = "select * from data_analysis_praiseuserstatistics where kdt_id =%s and from_wechat_user_id=%s"
    cursor.execute(sql,[data_dic['kdt_id'],data_dic['from_wechat_user_id']])
    res = cursor.fetchall()
    if res:
        sql = 'update data_analysis_praiseuserstatistics set  add_time_desc=%s ,add_time=%s,' \
              'wechat_msg_count=wechat_msg_count+%s,hf_msg_count=hf_msg_count+%s,' \
              'wechat_click_count=wechat_click_count+%s,hf_click_count=hf_click_count+%s ' \
              'where from_wechat_user_id=%s and kdt_id=%s'

        params = [data_dic['add_time_desc'] , data_dic['add_time'],data_dic['wechat_msg_count'],
                  data_dic['hf_msg_count'], data_dic['wechat_click_count'], data_dic['hf_click_count'],
                  data_dic['from_wechat_user_id'],data_dic['kdt_id']
                  ]
        cursor.execute(sql,params)
        conn.commit()
    else:
        dataimport(data_dic,table)