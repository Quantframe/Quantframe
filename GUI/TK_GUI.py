##----- Load public packages
import pandas as pd
import numpy as np
import Tkinter as TK
##----- Load local packages
sys.path.append("D:\Quant\Local\Strategy\Big_Brain\Data_Management\Download")
sys.path.append("D:\Quant\Local\Strategy\Big_Brain")
sys.path.append("D:\Quant\Local\Strategy\Big_Brain\kdb_python")

##-----加载JSON控制文件
with open('D:/Quant/Local/Strategy/Big_Brain/configs/config_GUI.json', 'r') as f: config_GUI = json.loads(f.read())

## GUI页面设计
'''
功能实现：
    1，策略书写模块
    2，策略回测曲线显示
    3，策略回测报告生成
    4，因子分析模块（从因子库中选择因子，因子选股分组测试，因子择时测试）
    5，策略实盘跟踪显示
    6，自己编写指标与走势的组合图显示
'''



class GUI():
    def __init__(self,config_GUI):
        '''
        初始化框架和功能类别
        '''
        self.root=TK.Tk() #创建主窗体
        self.root.wm_title(config_GUI['main_frame']['frame_name']) #定义平台名称

    def Main_menu(self):

    def Sub_menu(self):

    def