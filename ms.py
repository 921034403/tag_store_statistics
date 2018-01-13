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



mysql_option1 = {
    "host": "120.76.167.22",
    "port": 3306,
    "user": "idbear",
    "database":"ibi",
    "password": "Wai9yi_shaza!o2die2dai@z5g#ei",
    'cursorclass': pymysql.cursors.DictCursor,
    'charset': "utf8",
    "local_infile": True
}
conn1 = pymysql.connect(**mysql_option1)
cursor1 = conn1.cursor()

# print(calendar.monthrange(2002, 01))
# print(calendar.weekday(2018, 1, 8))