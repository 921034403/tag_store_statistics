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