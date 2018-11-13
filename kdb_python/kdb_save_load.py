
#---------------------------------------------

import qpython.qconnection as qp
from qpython.qconnection import MessageType
import pandas as pd
import numpy as np

import datetime as dt

#---------------------------------------------

def save_as_kdb(config,
                output_name,
                data,
                reset_index = True,
                set_index = True,
                index_name='trade_date'):
    
    if(reset_index):
        if index_name in data.columns.tolist(): pass
        else: data=data.reset_index()
    q=qp.QConnection(config['kdb_save']['host'],
                     config['kdb_save']['port'],
                     config['kdb_save']['username'], 
                     config['kdb_save']['password'])
    q.open()
    q('set', np.string_(output_name), data)
    query = 'save `$("" sv ("' + config['place_save']['kdb'] + '";"' + output_name + '"))'
    q.sync(query)
    q.close()
    if(set_index): data = data.set_index(index_name)
    else: pass

def load_from_kdb(config,
                read_name):
    q=qp.QConnection(config['kdb_load']['host'], 
                     config['kdb_load']['port'],
                     config['kdb_load']['username'], 
                     config['kdb_load']['password'])
    q.open()
    query='get `:'+config['place_load']['kdb']+read_name
    data=q.sync(query)
    data=pd.DataFrame(data)
    q.close()
    return data
