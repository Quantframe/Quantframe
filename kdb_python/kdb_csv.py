
#---------------------------------------------

import qpython.qconnection as qp
from qpython.qconnection import MessageType
import pandas as pd
import numpy as np
import kdb_save_load as ksl

import datetime as dt

#---------------------------------------------

def save(config, output_name, data,reset_index=True,set_index=False,index_name='trade_date'):
    if(config['save_as_csv']): 
        data.to_csv(config['place_save']['csv']+output_name+'.csv')
    if(config['save_as_kdb']):
        ksl.save_as_kdb(config,output_name, data,reset_index,set_index,index_name)

def load(config, read_name):
    if(config['use_csv_or_kdb']=='csv'):
        data = pd.read_csv(config['place_load']['csv']+read_name+'.csv')
    elif(config['use_csv_or_kdb']=='kdb'):
        data = ksl.load_from_kdb(config, read_name)
    return data

def update(config,update_kdb,update_csv,last_date_record,name,new_data,reset_index=True,set_index=False,index_name='trade_date'):
    # if update_kdb:
    #     data = ksl.load_from_kdb(config, name)
    #     data[index_name] += 946684800000000000
    #     data[index_name] = pd.to_datetime(data[index_name])
    #     data = data.set_index(index_name)
    # if update_csv:
    #     data = pd.read_csv(config['place_load']['csv']+name+'.csv')
    #     data = data.set_index(index_name)
    # new_data = new_data[new_data.index>=data.index.max()]
    if update_kdb:
        data_kdb_save = new_data[new_data.index>last_date_record['KDB'][name]]
        if data_kdb_save.shape[0]>0:
            if (config['runD']): print(dt.datetime.now(), '开始更新KDB数据：,过去文件最新日期: ' + str(last_date_record['KDB'][name]) + '; 更新开始日期: ' + str(data_kdb_save.index.min()))

            if (reset_index): data_kdb_save = data_kdb_save.reset_index()
            q = qp.QConnection(config['kdb_save']['host'],
                               config['kdb_save']['port'],
                               config['kdb_save']['username'],
                               config['kdb_save']['password'])
            q.open()
            query = name+': get`:'+config['place_save']['kdb']+name+';'
            q.sync(query)
            q('set', np.string_('table_new'), data_kdb_save)
            query = name+':'+name+' upsert table_new;'
            query += 'save `$("" sv ("' + config['place_save']['kdb'] + '";"' + name + '"))'
            q.sync(query)
            q.close()
        else: pass
    if update_csv:
        data_csv_save = new_data[new_data.index>last_date_record['CSV'][name]]
        if data_csv_save.shape[0] > 0:
            if (config['runD']): print(dt.datetime.now(), '开始更新CSV数据：,过去文件最新日期: ' + str(last_date_record['CSV'][name]) + '; 更新开始日期: ' + str(data_csv_save.index.min()))
            data = pd.read_csv(config['place_load']['csv']+name+'.csv')
            data = data.set_index(index_name)
            data = pd.concat([data,data_csv_save],axis=0,ignore_index=False)
            data.to_csv(config['place_save']['csv'] + name + '.csv')
        else: pass



