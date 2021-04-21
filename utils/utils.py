import numpy as np
import pandas as pd
import xarray as xr
import datetime
import logging

def throw_error(msg):
    '''
    throw error and exit
    '''
    logging.error(msg)
    exit()

def write_log(msg, lvl=20):
    '''
    write logging log to log file
    level code:
        CRITICAL    50
        ERROR   40
        WARNING 30
        INFO    20
        DEBUG   10
        NOTSET  0
    '''

    logging.log(lvl, msg)



def get_station_df(sta_path):
    '''get station meta info (lat, lon, elev)'''
    #区站号  站名    省份    站点类型    纬度(度分)  经度(度分)  观测场拔海高度(0.1米)   气压感应器拔海高度(0.1米)   起始时间    终止时间                             
    #50136   漠河    黑龙江  国家基准气候站  5258N   12231E  4330    4342    19570401    20121231
    
    df = pd.read_excel(sta_path)
    df=df.dropna()
    return(df)


def conv_deg(deg_str):
    '''convert to degree info'''
    value=int(deg_str)//100
    value=value+(int(deg_str)-value*100)/60
    return(value)


def get_mon_ano_xr(data_arr, clim_strt_time, clim_end_time, std=False):
    ''' calculate monthly anomaly '''
    clm_arr=data_arr.sel(time=slice(clim_strt_time, clim_end_time)).groupby('time.month').mean('time')
    std_arr=data_arr.sel(time=slice(clim_strt_time, clim_end_time)).groupby('time.month').std('time')

    if std:
        for ii in range(0,12):
            data_arr[ii::12,:,:]=(data_arr[ii::12,:,:]-clm_arr[ii,:,:])/std_arr[ii,:,:]
    else:
        for ii in range(0,12):
            data_arr[ii::12,:,:]=data_arr[ii::12,:,:]-clm_arr[ii,:,:]

    return data_arr

def get_mon_ano_pd(df, clim_strt_time, clim_end_time, std=False):
    ''' calculate monthly anomaly '''
    clim_df=df[(df.index>=clim_strt_time) & (df.index<=clim_end_time)]
    clim_df_anncyc = clim_df.groupby(clim_df.index.month).mean() # climatological annual cycle
    clim_std_df_anncyc = clim_df.groupby(clim_df.index.month).std() # clim std annual cycle
    
    df_dmean=pd.DataFrame()
    if std:
        # group by month and calculate deviation (over std)
        for name, group in  df.groupby(df.index.month):
            df_dmean=pd.concat([df_dmean,(group.apply(lambda x: (x-clim_df_anncyc.loc[name])/clim_std_df_anncyc.loc[name], axis=1))])
    else:
        for name, group in  df.groupby(df.index.month):   
            df_dmean=pd.concat([df_dmean,(group.apply(lambda x: (x-clim_df_anncyc.loc[name]), axis=1))])

    return df_dmean.sort_values(by='time')


def get_lag_ar2d(df, lag_step):
    """
        construct lag array 2d (n features x m samples)
        from -lag_step to -1
        args:
            df          dataframe contains series
            lag_step    how long the lag takes
        returns:
            X           lagged series, 2-D
            col_list    col names
 
    """
    org_col_list=df.columns.values.tolist()
    col_list=[itm+'_lag1' for itm in org_col_list]
    X_all = np.array(df.values)
    X=X_all[:-lag_step,:]
    for ii in range(1, lag_step):
        X_tmp=X_all[ii:(-lag_step+ii),:]
        X=np.concatenate((X,X_tmp),1)
        new_list=[itm+'_lag'+str(ii+1) for itm in org_col_list]
        col_list.extend(new_list)
    return X, col_list

def get_lag_ar1d(df, lag_step):
    """
        construct lag array 1d (1 feature and m samples)
        from -lag_step to -1
        args:
            df          dataframe contains series
            lag_step    how long the lag takes
        returns:
            X           lagged series, 2-D
            col_list    col names
    """
    
    try:
        array_name=df.columns.values.tolist()[0]
    except: 
        array_name=df.name # series 

    col_list=[array_name+'_lag1']

    X_all = np.array(df.values)
    X=X_all[:-lag_step]
    X=X[:,np.newaxis]   # change to 2-D
    for ii in range(1, lag_step):
        X_tmp=X_all[ii:(-lag_step+ii)]
        X_tmp=X_tmp[:,np.newaxis]
        X=np.concatenate((X,X_tmp),axis=1)
        new_list=[array_name+'_lag'+str(ii+1)]
        col_list.extend(new_list)
    return X, col_list

