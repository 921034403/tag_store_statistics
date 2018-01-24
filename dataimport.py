from ms import cursor,conn



def dataimport(data_dic,database):
    lis = [i for i in data_dic]
    key = ','.join(lis)
    _s = ','.join(['%s']*len(lis))
    values = [data_dic[i] for i in lis]
    sql = 'insert into {0}({1})VALUE({2}) '
    sql = sql.format(database,key,_s)
    cursor.execute(sql,values)
    conn.commit()
    # print(cursor.mogrify(sql,values))