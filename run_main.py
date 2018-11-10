# -*- coding: utf-8 -*-
#---------------------------------------------
'''
导入函数库位置
'''
import sys
sys.path.append('D:/Quant/Local/Strategy/Big_Brain/Data_Management/Download')
#---------------------------------------------
import qpython.qconnection as qp
from qpython.qconnection import MessageType
import pandas as pd
import numpy as np
import datetime as dt
import json

import kdb_csv as kc

import Download
#---------------------------------------------
'''
加载 config 文件
'''
with open('configs/config_main.json', 'r') as f:
    config_main = json.loads(f.read())
runD = config_main['runD']
if (runD): print(dt.datetime.now(), 'load config: config_main')
with open('configs/config_download.json', 'r') as f:
    config_download = json.loads(f.read())
if (runD): print(dt.datetime.now(), 'load config: config_download')




#---------------------------------------------
'''
下载数据
'''
if config_main['option']['whether_download_data'] == 1:
    if (runD): print(dt.datetime.now(), 'process: download data')
    Download.Download(config_download)
else:
    if (runD): print(dt.datetime.now(), 'process: give up download data')


'''
加载数据
'''


'''
处理数据
'''


