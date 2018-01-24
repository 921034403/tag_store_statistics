# _*_ coding:utf-8 _*_
from ms import conn,cursor



sql = 'select * from django_aip.third_part_wechat_store where mall_id =7 and store_type =1'
cursor.execute(sql)

res = cursor.fetchall()
for i in res:
    if i.get('location'):
        if i['location'][0] == u'L':
            store_area = i['location'][:2]
        else:
            store_area = i['location'][:2]+u'区'
        id = i['id']
        sql = 'update django_aip.third_part_wechat_store set store_area=%s where id = %s'
        cursor.execute(sql,[store_area,id])
        conn.commit()