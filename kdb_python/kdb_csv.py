
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

