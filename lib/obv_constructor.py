#/usr/bin/env python3
"""
    Build Observation Station Objects
    Class       
    ---------------
                obv
"""
from utils import utils

class obv:

    '''
    Construct Observation Station Objs
    Attributes
    -----------
    idx:        int, scalar
        in-situ station ID
    lat:        float, scalar
        in situ station lat
    lon:        float, scalar
        in situ station lon
    height:     float, scalar
        in situ station elevation above terrain
    Methods
    -----------
    '''
    
    def __init__(self, row):
        """ construct obv obj """
        self.sta_num=int(row['区站号'])
        self.province=row['省份']
        self.sta_type=row['站点类型']
        self.lat=utils.conv_deg(row['纬度(度分)'][0:-1])
        self.lon=utils.conv_deg(row['经度(度分)'][0:-1])
        self.height=int(row['观测场拔海高度(0.1米)'])/10.0
