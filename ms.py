# _*_ coding:utf-8 _*_
import pymysql,requests,re,sys
reload(sys)
sys.setdefaultencoding('utf-8')
try:
    IP = re.search('\d+\.\d+\.\d+\.\d+', requests.get("https://www.iplocation.net").text).group(0)
    print(u"获取IP成功")
except:
    print(u"获取IP失败")
    IP = "127.0.0.1"
if IP == "120.76.102.238":
    print(u"当前处于生产环境 ip: %s" % (IP))
    mysql_option = {
        "host": "119.23.153.216",
        "port": 3306,
        "user": "root",
        "database": "django_aip",
        "password": "gxtangyu",
        'cursorclass': pymysql.cursors.DictCursor,
        'charset': "utf8",
        "local_infile": True
    }
    praise_es = "django_aip_es_praise_v7"
    corpus_es = "django_aip_corpus_v2"
else:
    print(u"当前处于测试环境 ip: %s" % (IP))
    mysql_option = {
        "host": "119.23.142.224",
        "port": 3306,
        "user": "root",
        "password": "gxtangyu",
        "database": "django_aip",
        'cursorclass': pymysql.cursors.DictCursor,
        'charset': "utf8",
        "local_infile": True
    }
    praise_es = "django_aip_es_praise_v7(test)"
    corpus_es = "django_aip_corpus_v2(test)"


conn = pymysql.connect(**mysql_option)
cursor = conn.cursor()



