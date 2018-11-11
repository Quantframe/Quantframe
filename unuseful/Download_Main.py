
#---------------------------------------------

import qpython.qconnection as qp
from qpython.qconnection import MessageType
import pandas as pd
import numpy as np
import datetime as dt
import tushare as ts
import json
import re
import kdb_csv as kc
#---------------------------------------------
def Download(config_download):
    '''
    当前仅仅支持 股票 和 指数
    '''
    runD = config_download['runD']
    start_date = config_download['option_for_price']['start_date']
    end_date = config_download['option_for_price']['end_date']
    api = ts.pro_api(config_download['ts_api'])
    pattern = re.compile(r'\d+')

    if config_download['option_for_price']['whether_download_price'] == 1:
        if (runD): print(dt.datetime.now(), 'process: start download new table')
        for i in config_download['option_for_price']['asset']:
            '''
            获取 下载列表和列表基本信息
            '''
            if config_download['option_for_price']['products_list_' + i] == 1:
                if (runD): print(dt.datetime.now(),
                                 'start download new table: Get download list --- ' + i + ' (type: all)')
                if i == 'E':
                    basic_frame = api.stock_basic(exchange_id='', list_status='L', fields='ts_code,symbol,list_date')
                elif i == 'I':
                    basic_frame = pd.DataFrame()
                    for index_sub in ['MSCI', 'CSI', 'SSE', 'SZSE', 'CICC', 'SW', 'CNI', 'OTH']:
                        basic_frame_sub = api.index_basic(market=index_sub)
                        basic_frame = pd.concat([basic_frame, basic_frame_sub], axis=0, ignore_index=False)
                basic_list = list(set(basic_frame['ts_code']))
            else:
                if (runD): print(dt.datetime.now(), 'download new table: Get download list --- ' + i + ' (type: part)')
                basic_frame = pd.DataFrame()
                basic_list = config_download['option_for_price']['products_list_' + i]

            for freq in config_download['option_for_price']['freq']:
                for adj in config_download['option_for_price']['adj']:
                    '''
                    开始分类下载
                    '''
                    data_price = pd.DataFrame()
                    COUNT = 0
                    for item in basic_list:
                        data_price_sub = pd.DataFrame(ts.pro_bar(pro_api=api, ts_code=item, asset=i, freq=freq, adj=adj,
                                                    start_date=start_date, end_date=end_date))
                        if data_price_sub.shape[0] > 0:
                            data_price_sub['trade_date'] = pd.to_datetime(data_price_sub['trade_date'])
                            data_price_sub = data_price_sub.sort_values(by = 'trade_date')
                            item_res = re.findall(pattern, item)[0]
                            out_put_name_sub = 'trade_' + i + '_' + freq + '_' + adj + '_'+item_res+'_'+ str(start_date) + '_' + str(end_date)
                            kc.save(config_download, out_put_name_sub, data_price_sub, reset_index=False,set_index = False,index_name = 'trade_date')
                            data_price = pd.concat([data_price, data_price_sub], axis=0, ignore_index=False)
                            if (runD): print(dt.datetime.now(),'download new table: asset: ' + i + '; code: ' + item + '; freq: ' + freq + '; adj: ' + adj+'; processing: '+str(COUNT*100/len(basic_list))+'%')
                        else:
                            if (runD): print(dt.datetime.now(),'download new table: asset: ' + i + '; code: ' + item + '; freq: ' + freq + '; adj: ' + adj+'; processing: '+str(COUNT*100/len(basic_list))+'%; no new table')

                        COUNT += 1

                    if basic_frame.shape[0] > 0:
                        data_price = pd.merge(basic_frame, data_price)
                    else:
                        pass

                    if config_download['option_for_price']['products_list_' + i] == 1:
                        out_put_name = 'trade_' + i + '_' + freq + '_' + adj + '_' + str(start_date) + '_' + str(
                            end_date)
                    else:
                        out_put_name = 'trade_' + i + '_' + freq + '_' + adj + '_' + str(start_date) + '_' + str(
                            end_date)

                    kc.save(config_download, out_put_name, data_price, reset_index=False,set_index = False,index_name = 'trade_date')
                    if (runD): print(dt.datetime.now(), 'download new table: save successfully: ' + out_put_name)

    else:
        pass


