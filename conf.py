# -*- coding:utf-8 -*-
import os
from pylab import mpl


mpl.rcParams["font.sans-serif"] = ["SimHei"]
mpl.rcParams["axes.unicode_minus"] = False

TOKEN = "64f5c714a9bdac08dac8072ce9a5b91a837e0b65590965b28a9a52a1"

BASE_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)))

DATA_PATH = os.path.join(BASE_DIR, "data")

SQLITE_PATH = "sqlite:///{}".format(os.path.join(DATA_PATH, "tushare.db"))
TASK_NO = 100

DB = "127.0.0.1"
PORT = "3306"
USERNAME = "root"
PASSWORD = "toor"
# MYSQL_PATH = "mysql+pymysql://root@127.0.0.1:3306/digchouti?charset=utf8"


# pro = ts.pro_api(TOKEN)
