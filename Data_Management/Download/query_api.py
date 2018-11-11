##----- Load public packages
import pandas as pd
import numpy as np
import json
import tushare as ts
import qpython.qconnection as qp
from qpython.qconnection import MessageType
##----- Load local packages

class ts_api():
    def __init__(self,config_download):
        self.config = config_download
        self.api =  ts.pro_api(self.config['ts_api'])
        self.pro_api = self.config['ts_api']

    def get_stock_code_list(self,exchange='',list_status='L',fields='ts_code'):
        name_list = self.api.stock_basic(exchange=exchange, list_status=list_status, fields=fields)
        name_list = list(name_list['ts_code'].values)
        return name_list

    def get_index_code_list(self):
        name_list = []
        for i in self.config['HDB_option']['Index']['index_market_list']:
            name_sub = list(self.api.index_basic(market=i)['ts_code'])
            name_list.extend(name_sub)
        return name_list

    def get_hist_OHLCV(self,ts_code,frequency,asset,adj,start_date,end_date=''):
        data = ts.pro_bar(
                        pro_api = self.api,
                        ts_code = ts_code, ## CODE.TYPE [TYPE: SZ,SH]
                        freq = frequency, ## 1MIN,5MIN,15MIN,30MIN,60MIN,D
                        asset = asset, ##E,I,C,F,O
                        adj=adj,  ## qfq,hfq,None
                        start_date = start_date, ##YYYYMMDD
                        end_date = end_date) #YYYYMMDD
        if data.shape[0] > 0:
            data = data.set_index(pd.to_datetime(data[self.config['datetime_name']]))[['ts_code','open','high','low','close','vol','amount']]

            data_adj_factor = self.api.adj_factor(
                                                ts_code=ts_code, ## CODE.TYPE [TYPE: SZ,SH]
                                                start_date=start_date, ##YYYYMMDD
                                                end_date=end_date) ##YYYYMMDD
            data_adj_factor = data_adj_factor.set_index(pd.to_datetime(data_adj_factor[self.config['datetime_name']]))[['adj_factor']]
            data = pd.merge(data,data_adj_factor,left_index=True,right_index=True,how='outer')
            return data
        else: print('数据缺失: 代码: '+str(ts_code))

