
#---------------------------------------------

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
#-------------------------------------------- load configs & add paths
sys.path.append("D:\Quant\Local\Strategy\Big_Brain\Data_Management\Download")
sys.path.append("D:\Quant\Local\Strategy\Big_Brain")
sys.path.append("D:\Quant\Local\Strategy\Big_Brain\kdb_python")

#--------------------------------------------
import kdb_csv as kc
import query_api

#---------------------------------------------
'''
Notes:
    1, HDB 每天在收盘后运行
    2，HDB参数应包括
    {【开关】：【是否自动开机运行】
        【参数（如果自动开机运行）】：【每日运行的时间点】}
    3，HDB运行后会自动检查缺失数据并补充缺失数据
    4，HDB 开机时间应该在收盘半小时后，给RDB将数据存入HDB的时间
'''
with open('D:/Quant/Local/Strategy/Big_Brain/configs/config_download.json', 'r') as f: config_download = json.loads(f.read())

class HDB_Core():
    def __init__(self,config_download):
        self.config = config_download
        self.save_use_csv_or_kdb = self.config['use_csv_or_kdb']
    def reset_HDB_check(self):
        self.kdb_initial_download_list = []
        self.csv_initial_download_list = []
        self.kdb_initial_pass_list = []
        self.csv_initial_pass_list = []
        self.kdb_update_list = []
        self.csv_update_list = []
        self.whether_download_initial_KDB = 0
        self.whether_download_initial_CSV = 0
        self.whether_update_KDB = 0
        self.whether_update_CSV = 0
        self.today = pd.to_datetime(dt.date.today())
        self.config['files_stored'] = []
        self.last_date_record = dict()
        for i in self.config['HDB_option']:
            if self.config['HDB_option'][i]['whether_download']:
                for h in self.config['HDB_option'][i]['save_type']:
                    for j in self.config['HDB_option'][i]['freq']:
                        for k in self.config['HDB_option'][i]['asset']:
                            name_files_stored = h+'_'+j+'_'+k
                            self.config['files_stored'].append(name_files_stored)

    def get_last_date_of_file(self,file_name,file_type):
        if file_type == 'KDB':
            self.config['use_csv_or_kdb'] = 'kdb'
            data = kc.load(self.config,file_name)
            self.config['use_csv_or_kdb'] = self.save_use_csv_or_kdb
            data[self.config['datetime_name']] += 946684800000000000
            data[self.config['datetime_name']] = pd.to_datetime(data[self.config['datetime_name']])
            last_date_check = pd.to_datetime(data[self.config['datetime_name']].max().date())

        if file_type == 'CSV':
            self.config['use_csv_or_kdb'] = 'csv'
            data = kc.load(self.config,file_name)
            self.config['use_csv_or_kdb'] = self.save_use_csv_or_kdb
            last_date_check = pd.to_datetime(data[self.config['datetime_name']].max().date())
        return last_date_check,check
    # def check_KDB_CSV_match(self):
    #     last_date_check_KDB = self.get_last_date_of_file(self,file_name,file_type)
    # def download_data(self):

    def download_combing(self,download_info,start_date):
        stock_code_list = ts_api.get_stock_code_list()
        count_steps = 0
        for code in stock_code_list:
            data_sub = ts_api.get_hist_OHLCV(
                                            code,
                                            frequency=download_info[1],
                                            asset=download_info[2],
                                            adj=self.config['HDB_option'][download_info[2]]['adj'],
                                            start_date=start_date
                                            )
            data_combine = pd.concat([data_combine, data_sub], axis=0, ignore_index=False)
            if (self.config['runD']): print(dt.datetime.now(), '加载完成：代码: ' + str(code) + '; 进度: ' + str(count_steps * 100 / (len(stock_code_list))) + '%;')
        return data_combine

    def check_HDB_update(self):
        if self.config['save_as_kdb']:
            if (self.config['runD']): print(dt.datetime.now(), '检查: 是否存在原始KDB文件')
            dir = os.listdir(self.config['place_save']['kdb_root'] + self.config['place_save']['kdb'])
            for i in self.config['files_stored']:
                if i in dir: self.kdb_initial_pass_list.append(i)
                else: self.kdb_initial_download_list.append(i)
            if len(self.kdb_initial_download_list) > 0:
                if (self.config['runD']): print(dt.datetime.now(), '检查: 缺失以下KDB文件: 将下载最新的以下KDB文件: '+str(self.kdb_initial_download_list))
                self.whether_download_initial_KDB = 1
            else:
                if (self.config['runD']): print(dt.datetime.now(), '检查: 不缺少原始KDB文件,开始检查文件是否最新')

            for i in self.kdb_initial_pass_list:
                last_date_check = self.get_last_date_of_file(i,'KDB')
                self.last_date_record['KDB'][i] = last_date_check

                if last_date_check>= self.today:
                    if (self.config['runD']): print(dt.datetime.now(), '检查: KDB文件已经是最新: '+str(i)+'; HDB最新日期: '+str(last_date_check)+'; 当前日期: '+str(self.today))
                else:
                    if (self.config['runD']): print(dt.datetime.now(), '检查: KDB文件需要更新: '+str(i)+'; HDB最新日期: '+str(last_date_check)+'; 当前日期: '+str(self.today))
                    self.whether_update_KDB = 1
                    self.kdb_update_list.append(i)

        else:
            if (self.config['runD']): print(dt.datetime.now(), '检查: 跳过检查原始KDB文件')

        if self.config['save_as_csv']:
            if (self.config['runD']): print(dt.datetime.now(), '检查: 是否存在原始CSV文件')
            dir = os.listdir(self.config['place_save']['csv'])
            for i in self.config['files_stored']:
                if i in dir: self.csv_initial_pass_list.append(i)
                else: self.csv_initial_download_list.append(i)
            if len(self.csv_initial_download_list) > 0:
                if (self.config['runD']): print(dt.datetime.now(), '检查: 缺失以下CSV文件: 将下载最新的以下CSV文件: '+str(self.csv_initial_download_list))
                self.whether_download_initial_CSV = 1
            else:
                if (self.config['runD']): print(dt.datetime.now(), '检查: 不缺少原始CSV文件,开始检查文件是否最新')

            for i in self.csv_initial_pass_list:
                last_date_check = self.get_last_date_of_file(i, 'CSV')
                self.last_date_record['CSV'][i] = last_date_check

                if last_date_check>= self.today:
                    if (self.config['runD']): print(dt.datetime.now(), '检查: CSV文件已经是最新: '+str(i)+'; HDB最新日期: '+str(last_date_check)+'; 当前日期: '+str(self.today))
                else:
                    if (self.config['runD']): print(dt.datetime.now(), '检查: CSV文件需要更新: '+str(i)+'; HDB最新日期: '+str(last_date_check)+'; 当前日期: '+str(self.today))
                    self.whether_update_CSV = 1
                    self.csv_update_list.append(i)
        else:
            if (self.config['runD']): print(dt.datetime.now(), '检查: 跳过检查原始CSV文件')

    def update_HDB(self):
        if (self.config['runD']): print(dt.datetime.now(), '开始下载&更新数据######################################################')
        if (self.config['runD']): print(dt.datetime.now(), '下载列表：KDB: '+str(self.kdb_initial_download_list))
        if (self.config['runD']): print(dt.datetime.now(), '下载列表：CSV: '+str(self.csv_initial_download_list))
        if (self.config['runD']): print(dt.datetime.now(), '更新列表：KDB: '+str(self.kdb_update_list))
        if (self.config['runD']): print(dt.datetime.now(), '更新列表：CSV: '+str(self.csv_update_list))
        '''
        整合下载列表,注册tushare
        '''
        self.initial_download_list = set(self.kdb_initial_download_list + self.csv_initial_download_list)
        self.update_list = set(self.kdb_update_list + self.csv_update_list)
        ts_api = query_api.ts_api(config_download)
        '''
        下载HDB
        '''
        if set(self.initial_download_list) > 0:

            for i in self.initial_download_list:
                data_combine = pd.DataFrame()
                download_info = i.split('_')
                if (self.config['runD']): print(dt.datetime.now(), '开始下载初始数据：文件名: ' + str(i)+'; 文件名分割：'+str(download_info))
                data_combine = self.download_combing(download_info,self.config['HDB_option'][download_info[2]]['start_date'])
                kc.save(self.config,i,data_combine)
        '''
        更新HDB
        '''
        if set(self.update_list) > 0:
            for i in self.update_list:
                if (self.config['runD']): print(dt.datetime.now(), '开始更新数据：文件名: ' + str(i))
                if (self.config['save_as_csv']) & (self.config['save_as_kdb']):############# KDB CSV 都更新
                    update_kdb = 1
                    update_csv = 1
                    if (self.config['runD']): print(dt.datetime.now(), '开始更新数据：KDB和CSV均需要更新，检查两种文件数据是否相符: ' + str(i))
                    if self.last_date_record['KDB'][i] == self.last_date_record['CSV'][i]:
                        if (self.config['runD']): print(dt.datetime.now(),'两种文件最后日期相符')
                        update_start_date = str((self.last_date_record['KDB'][i] + dt.timedelta(days=1)).date()).replace('-', '')
                        data_combine = self.download_combing(download_info,update_start_date)
                        kc.update(self.config,update_kdb,update_csv,self.last_date_record,i,data_combine)
                    elif self.last_date_record['KDB'][i] != self.last_date_record['CSV'][i]:
                        if (self.config['runD']): print(dt.datetime.now(),'两种文件最后日期不符')
                        update_start_date = str((min(self.last_date_record['KDB'][i],self.last_date_record['KDB'][i]) + dt.timedelta(days=1)).date()).replace('-', '')
                        data_combine = self.download_combing(download_info,update_start_date)
                        kc.update(self.config,update_kdb,update_csv,self.last_date_record,i,data_combine)

                elif (self.config['save_as_csv']==0) & (self.config['save_as_kdb']): ############# KDB 更新； 不更新 CSV
                    update_kdb = 1
                    update_csv = 0
                    if (self.config['runD']): print(dt.datetime.now(), '开始更新数据：更新KDB不更新CSV: ' + str(i))
                    update_start_date = str((self.last_date_record['KDB'][i] + dt.timedelta(days=1)).date()).replace('-', '')
                    data_combine = self.download_combing(download_info,update_start_date)
                    kc.update(self.config,update_kdb,update_csv,self.last_date_record,i,data_combine)

                elif (self.config['save_as_csv']) & (self.config['save_as_kdb']==0):############# CSV 更新; 不更新 KDB
                    update_kdb = 0
                    update_csv = 1
                    if (self.config['runD']): print(dt.datetime.now(), '开始更新数据：更新KDB不更新CSV: ' + str(i))
                    update_start_date = str((self.last_date_record['KDB'][i] + dt.timedelta(days=1)).date()).replace('-', '')
                    data_combine = self.download_combing(download_info, update_start_date)
                    kc.update(self.config, update_kdb, update_csv,self.last_date_record, i, data_combine)

if __name__ == "__main__":
    HDB_Core = HDB_Core(config_download)
    HDB_Core.reset_HDB_check()
    HDB_Core.check_HDB_update()
    HDB_Core.update_HDB()



