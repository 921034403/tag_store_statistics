import pymysql

import calendar

mysql_option = {
    "host": "119.23.153.216",
    "port": 3306,
    "user": "root",
    "database":"django_aip",
    "password": "gxtangyu",
    'cursorclass': pymysql.cursors.DictCursor,
    'charset': "utf8",
    "local_infile": True
}
conn = pymysql.connect(**mysql_option)
cursor = conn.cursor()




mysql_option = {
    "host": "119.23.153.216",
    "port": 3306,
    "user": "root",
    "database":"django_aip",
    "password": "gxtangyu",
    'cursorclass': pymysql.cursors.DictCursor,
    'charset': "utf8",
    "local_infile": True
}
conn = pymysql.connect(**mysql_option)
cursor = conn.cursor()



