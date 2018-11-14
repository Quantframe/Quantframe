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

    def get_trading_calandar(self,start_date,end_date='',exchange=''):
        calendar = self.api.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date)
        calendar = calendar[calendar['is_open'] == 1]['cal_date'].tolist()
        return calendar

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

    def get_hist_OHLCV(self,hist_or_real,ts_code='',frequency='',asset='',adj='',start_date='',end_date='',trade_date=''):
        if hist_or_real == 'hist':
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
        elif hist_or_real == 'real':
            if asset == 'E':
                data = self.api.query('daily', ts_code=ts_code, start_date=start_date, end_date=end_date,trade_date=trade_date)
            elif asset == 'I':
                data = self.api.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date,trade_date = trade_date)
            if data.shape[0] > 0:
                data = data[[self.config['datetime_name'],'ts_code','open','high','low','close','vol','amount']]
                data_adj_factor = self.api.adj_factor(
                                                    ts_code=ts_code,  ## CODE.TYPE [TYPE: SZ,SH]
                                                    trade_date=trade_date)  ##YYYYMMDD
                data_adj_factor = data_adj_factor[['ts_code','adj_factor']]
                data = pd.merge(data, data_adj_factor, left_on='ts_code', right_on='ts_code', how='outer')
                data[self.config['datetime_name']] = pd.to_datetime(data[self.config['datetime_name']])
                data = data.set_index(self.config['datetime_name'])
                return data

            else: print('数据缺失: 代码: ' + str(ts_code))

    def get_fundamental(self,ts_code='',start_date='',end_date='',trade_date=''):
        data = self.api.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date,trade_date=trade_date)
        data = data.set_index(pd.to_datetime(data[self.config['datetime_name']]))
        return data
