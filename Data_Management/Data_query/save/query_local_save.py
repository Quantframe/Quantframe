##----- Load public packages
import pandas as pd
import numpy as np
import json
import datetime as dt
import qpython.qconnection as qp
from qpython.qconnection import MessageType
import threading as th
import multiprocessing as mp
##----- Load local packages

class DataFrame_to_Panel():
    def get_column(self, df, column, ts_code):
        df = pd.DataFrame(df[df['ts_code'] == ts_code])
        df = pd.DataFrame({ts_code: df[column].values.tolist()}, index=df.index)
        return df

    def to_Panel(self, data):
        column_list = data.columns.tolist()
        column_list.remove('ts_code')
        for column in column_list:
            frame = [self.get_column(data, column, x) for x in data.ts_code.unique()]
            globals()['res_' + column] = pd.concat(frame, axis=1)
        data_Panel = pd.Panel(dict([(column, globals()['res_' + column]) for column in column_list]))
        return data_Panel


def get_OHLCV_by_kdb(config_data_query,
                     code=[],
                     frequency='',
                     asset='',
                     start_date='',
                     end_date='',
                     trade_date=''):

    file_name = 'OHLCV_' + frequency + '_' + asset
    code = ''.join(list(map(lambda x: '`' + x, code)))
    if start_date != '': start_date = pd.to_datetime(start_date).strftime('%Y.%m.%d')
    if end_date != '': end_date = pd.to_datetime(end_date).strftime('%Y.%m.%d')
    if trade_date != '': trade_date = pd.to_datetime(trade_date).strftime('%Y.%m.%d')

    q = qp.QConnection(config_data_query['kdb_load']['host'],
                       config_data_query['kdb_load']['port'],
                       config_data_query['kdb_load']['username'],
                       config_data_query['kdb_load']['password'])
    q.open()

    if len(code) != 0:
        if (start_date == '') & (end_date == ''):
            if trade_date == '': query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where ts_code in ' + code
            else: query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where ts_code in ' + code+',trade_date in '+trade_date

        elif (start_date != '') & (end_date == ''): query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where ts_code in ' + code + ', trade_date >= ' + start_date
        elif (start_date == '') & (end_date != ''): query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where ts_code in ' + code + ', trade_date <= ' + end_date
        elif (start_date != '') & (end_date != ''): query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where ts_code in ' + code + ', trade_date >= ' + start_date + ', trade_date <= ' + end_date
    else:
        if (start_date == '') & (end_date == ''):
            if trade_date == '': query = '0!data:select from (get`:Local/OHLCV_D_E)'
            else: query = '0!data:select from (get`:Local/OHLCV_D_E) where trade_date in '+trade_date
        elif (start_date != '') & (end_date == ''): query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where trade_date >= ' + start_date
        elif (start_date == '') & (end_date != ''): query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where trade_date <= ' + end_date
        elif (start_date != '') & (end_date != ''): query = '0!data:select from (get`:' + config_data_query['place_load']['kdb'] + file_name + ') where trade_date >= ' + start_date + ', trade_date <= ' + end_date

    data = pd.DataFrame(q.sync(query))

    q.close()
    data['trade_date'] += 946684800000000000
    data['trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index('trade_date')
    data = data.dropna()
    return data

def get_OHLCV_by_csv(config_data_query,
                     code=[],
                     frequency='',
                     asset='',
                     start_date='',
                     end_date='',
                     trade_date=''):
    file_name = 'OHLCV_' + frequency + '_' + asset
    if start_date != '': start_date = pd.to_datetime(start_date)
    if end_date != '': end_date = pd.to_datetime(end_date)
    if trade_date != '': trade_date = pd.to_datetime(trade_date)

    data = pd.read_csv(config_data_query['place_load']['csv'] + file_name + '.csv')
    data['trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.set_index('trade_date')

    if len(code) != 0:
        data = data[data['ts_code'].isin(code)]
        if (start_date == '') & (end_date == ''):
            if trade_date == '': pass
            else: data = data[data.index==trade_date]
        elif (start_date != '') & (end_date == ''): data = data[data.index>=start_date]
        elif (start_date == '') & (end_date != ''): data = data[data.index<=end_date]
        elif (start_date != '') & (end_date != ''): data = data[(data.index>=start_date)&(data.index<=end_date)]
    else:
        if (start_date == '') & (end_date == ''):
            if trade_date == '': pass
            else: data = data[data.index==trade_date]
        elif (start_date != '') & (end_date == ''): data = data[data.index>=start_date]
        elif (start_date == '') & (end_date != ''): data = data[data.index<=end_date]
        elif (start_date != '') & (end_date != ''): data = data[(data.index>=start_date)&(data.index<=end_date)]

    data = data.dropna()
    return data


def data_initialize(DataFrame_to_Panel,
                    config_data_query,
                    data,adj='',
                    items=['ts_code','open','high','low','close','vol','adj_factor'],
                    is_panel=0,
                    multi_thread=0,
                    multi_process=0):

    if adj == 'hfq':
        data['open'] = data['open'] * data['adj_factor']
        data['high'] = data['high'] * data['adj_factor']
        data['low'] = data['low'] * data['adj_factor']
        data['close'] = data['close'] * data['adj_factor']

    elif adj == 'qfq':
        print(
            '#########################################################################################################')
        print('尚在解决')
        print(
            '#########################################################################################################')

    for i in data.columns.tolist():
        if (type(data[i][0]) == bytes):
            data[i] = list(map(lambda x: x.decode(encoding='UTF-8'), data[i]))
    data = data[items]

    if is_panel == 0:
        pass
    else:
        DataFrame_to_Panel = DataFrame_to_Panel()
        data = DataFrame_to_Panel.to_Panel(data)

    return data

def get_OHLCV(config_data_query,
              use_csv_or_kdb='kdb',
              code = [],
              frequency = '',
              asset = '',
              adj = '',
              start_date = '',
              end_date = '',
              trade_date = '',
              items=['ts_code','open','high','low','close','vol','adj_factor'],
              is_panel=0,
              multi_thread=0,
              multi_process=0):
    if is_panel:
        aryx = 3
    else:
        aryx = 2
    start_time = dt.datetime.now()
    if use_csv_or_kdb == 'kdb': data = get_OHLCV_by_kdb(config_data_query,
                                                        code=code,
                                                        frequency=frequency,
                                                        asset=asset,
                                                        start_date=start_date,
                                                        end_date=end_date,
                                                        trade_date=trade_date)

    elif use_csv_or_kdb == 'csv': data = get_OHLCV_by_csv(config_data_query,
                                                        code=code,
                                                        frequency=frequency,
                                                        asset=asset,
                                                        start_date=start_date,
                                                        end_date=end_date,
                                                        trade_date=trade_date)

    data = data_initialize(DataFrame_to_Panel,
                           config_data_query,
                           data,
                           adj=adj,
                           items=items,
                           is_panel=is_panel)
    end_time = dt.datetime.now()
    if 'ts_code' in items: items.remove('ts_code')

    if (config_data_query['runD']): print(dt.datetime.now(), '提取数据成功, 载入数据花费时间：'+str(end_time - start_time)+'数据维度:' + str(aryx) + '; 标的数：' + str(
        len(code)) + '; 起始时间：' + str(data[items[0]].index.min()) + '; 结束时间：' + str(data[items[0]].index.max()))
    return data

if __name__=='__main__':
    import qpython.qconnection as qp
    from qpython.qconnection import MessageType
    import pandas as pd
    import numpy as np
    import datetime as dt
    import tushare as ts
    import json
    import re
    import os
    import sys
    # -------------------------------------------- load configs
    '''
    此处可视情况更换参数
    '''
    path_configs = 'D:/Quant/Local/Strategy/Big_Brain/configs/'
    config_list = ['config_main', 'config_download', 'config_data_query', 'config_paths', 'config_GUI']

    for item in config_list:
        with open(path_configs + item + '.json', 'r') as f: locals()[item] = json.loads(f.read())
    # -------------------------------------------- add local package paths
    for local_package_path in config_paths.keys():
        local_package_path.replace('/', '\\')
        sys.path.append(config_paths[local_package_path])

    data = get_OHLCV(
        config_data_query,
        use_csv_or_kdb='kdb',
        code=[],
        frequency='D',
        asset='E',
        adj='hfq',
        start_date='',
        end_date='',
        trade_date='',
        items=['ts_code', 'open', 'high', 'low', 'close', 'vol', 'adj_factor'],
        is_panel=0,
        multi_thread=0,
        multi_process=0)

