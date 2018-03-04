import pymysql

import calendar

mysql_option = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "sls",
    "database":"django_aip",
    "password": "ggsmd",
    'cursorclass': pymysql.cursors.DictCursor,
    'charset': "utf8",
    "local_infile": True
}
conn = pymysql.connect(**mysql_option)
cursor = conn.cursor()



