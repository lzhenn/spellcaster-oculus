#/usr/bin/env python
"""Data ETL from raw reanalysis/dyn forecast/station data"""

import numpy as np
import pandas as pd
import datetime
import xarray as xr
from lib import obv_constructor
from utils import utils

print_prefix='lib.ETL>>'

class etl_manager():
    '''
    Construct ETL manager 
    
    Attributes
    -----------

    Methods
    '''
 
    def __init__(self, cfg):
        ''' init etl manager '''
        ra_root=cfg['INPUT_OPT']['oculus_root']+cfg['INPUT_OPT']['ra_dir']
        self.ts_path=ra_root+cfg['INPUT_OPT']['ra_ts_name']
        self.hgt_path=ra_root+cfg['INPUT_OPT']['ra_hgt_name']
        self.pr_path=ra_root+cfg['INPUT_OPT']['ra_pr_name']
        self.ncc_path=cfg['INPUT_OPT']['oculus_root']+cfg['INPUT_OPT']['ncc_dir']
        self.all_flag=cfg['PRELOAD_OPT'].getboolean('exe_compute_all')
        self.single_sta_path=cfg['INPUT_OPT']['oculus_root']+'/feature_warehouse/single_station/'
        self.lag_step=int(cfg['CORE']['lag_step'])
 
        start_time=self.start_time=datetime.datetime.strptime(cfg['INPUT_OPT']['strt_time'], '%Y%m%d')
        end_time=self.end_time=datetime.datetime.strptime(cfg['INPUT_OPT']['end_time'], '%Y%m%d')

        self.clim_start_time=datetime.datetime.strptime(cfg['INPUT_OPT']['clim_strt_time'], '%Y%m%d')
        self.clim_end_time=datetime.datetime.strptime(cfg['INPUT_OPT']['clim_end_time'], '%Y%m%d')
    
        self.model_start_time=datetime.datetime.strptime(cfg['INPUT_OPT']['model_strt_time'], '%Y%m%d')
        self.model_end_time=datetime.datetime.strptime(cfg['INPUT_OPT']['model_end_time'], '%Y%m%d')
    

        self.time_frames=pd.date_range(start=start_time, end=end_time, freq='MS').to_period()
        # date series with day YYYY-MM-DD
        self.date_series=pd.date_range(start=start_time, end=end_time, freq='MS')
 
        
        # construct station meta data
        sta_meta=utils.get_station_df('./db/station_meta.xls')
            
        self.sta_lst=[]
        for idx, row in sta_meta.iterrows(): 
            self.sta_lst.append(obv_constructor.obv(row))

    def etl_ncc(self):
        ''' data etl the NCC index '''
        utils.write_log(print_prefix+'parsing NCC Index...')
        clim_strt_time, clim_end_time=self.clim_start_time, self.clim_end_time
        
        # load ncc series
        parser = lambda date: datetime.datetime.strptime(date, '%Y%m')
        flib_atm=pd.read_csv(self.ncc_path+'M_Atm_Nc.txt', sep='\s+',parse_dates=True, date_parser=parser)
        flib_ocn=pd.read_csv(self.ncc_path+'M_Oce_Er.txt', sep='\s+',parse_dates=True, date_parser=parser)
        flib_ext=pd.read_csv(self.ncc_path+'M_Ext.txt', sep='\s+',parse_dates=True, date_parser=parser)

        flib_atm, flib_ocn, flib_ext=flib_atm.add_prefix('atm_'), flib_ocn.add_prefix('ocn_'), flib_ext.add_prefix('ext_')       

        # ETL
        flib_all=pd.concat([flib_atm, flib_ocn, flib_ext], axis=1)
        flib_all=flib_all[self.start_time:self.end_time]
        flib_all.index.name='time'
        # raw data with -999 marked as filled value
        flib_all=(flib_all[flib_all!=-999])
        flib_all=flib_all.dropna(axis=1,how='any')

        flib_all=utils.get_mon_ano_pd(flib_all, clim_strt_time, clim_end_time, std=True)
        
        flib_all.to_csv('feature_warehouse/cir_idx.csv')

    def etl_giss(self):
        ''' get giss temp according to station meta '''
        utils.write_log(print_prefix+'parsing giss anomaly...')
        clim_strt_time, clim_end_time=self.clim_start_time, self.clim_end_time
        
        ds = xr.open_dataset(self.ts_path)
        var1 = ds['air'].sel(time=slice(self.start_time, self.end_time))
        var1 = utils.get_mon_ano_xr(var1, clim_strt_time, clim_end_time)
        col_list=[]
        all_df=pd.DataFrame()
        for idx, sta in enumerate(self.sta_lst):
            giss_ts=var1.sel(lat=float(sta.lat), lon=float(sta.lon), method='nearest').to_pandas()
            all_df=pd.concat([all_df, giss_ts], axis=1)
            col_list.append(str(sta.sta_num))
        all_df.columns=col_list
        all_df.index.name='time'
        all_df.to_csv('feature_warehouse/giss.csv')
    
    
    def etl_prec(self):
        ''' get prec according to station meta '''
        utils.write_log(print_prefix+'parsing prec anomaly...')
        clim_strt_time, clim_end_time=self.clim_start_time, self.clim_end_time
        
        ds = xr.open_dataset(self.pr_path)
        var1 = ds['precip'].sel(time=slice(self.start_time, self.end_time))
        var1 = utils.get_mon_ano_xr(var1, clim_strt_time, clim_end_time)
        col_list=[]
        all_df=pd.DataFrame()
        for idx, sta in enumerate(self.sta_lst):
            prec_ts=var1.sel(lat=float(sta.lat), lon=float(sta.lon), method='nearest').to_pandas()
            all_df=pd.concat([all_df, prec_ts], axis=1)
            col_list.append(str(sta.sta_num))
        all_df.columns=col_list
        all_df.index.name='time'
        all_df.to_csv('feature_warehouse/precip.csv')

    def load_giss(self):
        utils.write_log(print_prefix+'loading giss.csv...')

        self.giss=pd.read_csv('feature_warehouse/giss.csv',parse_dates=True, index_col='time')

    def load_prec(self):
        utils.write_log(print_prefix+'loading precip.csv...')
        self.precip=pd.read_csv('feature_warehouse/precip.csv')


    def etl_dyn(self):
        pass
        
    def etl_idx(self, time_frames):
        if self.all_flag:
            ds_hgt=xr.load_dataset(self.hgt_path)
            hgt_tgt=ds_hgt['hgt'][:,:,::-1,:] # reverse lat from -90,90
            hgt_tgt=hgt_tgt.sel(time=time_frames, level=500, method="nearest")
            hgt_tgt=hgt_tgt.sel(lat=slice(30,40), lon=slice(75,105))
            hgt_ano=utils.get_mon_ano(hgt_tgt, std=True)
            #self.flib['tpi_b']=hgt_ano.mean(('lat','lon')).values
            
            print(self.flib)


def gen_one_staXY_ts(mgr, sta_num):
    '''generate single station XY series'''
    sta_path=mgr.single_sta_path
    lag=mgr.lag_step
    clim_strt_time, clim_end_time=mgr.clim_start_time, mgr.clim_end_time
    model_strt_time, model_end_time=mgr.model_start_time, mgr.model_end_time
    
    model_date_series=pd.date_range(start=model_strt_time, end=model_end_time, freq='MS')
    
    # load station series
    parser = lambda date: datetime.datetime.strptime(date, '%Y-%m')
    sta_series=pd.read_csv(sta_path+'Tave/'+sta_num+'.txt',parse_dates=True, date_parser=parser, index_col='time')
    sta_series=sta_series[model_strt_time:model_end_time]
    sta_series=utils.get_mon_ano_pd(sta_series, clim_strt_time, clim_end_time, std=True)
    
    # check if station has complete series 
    print(model_date_series.shape[0])
    print(sta_series.shape[0])
    if model_date_series.shape[0]!=sta_series.shape[0]:
        sta_series=mgr.giss[model_strt_time:model_end_time][sta_num]
    X_auto, auto_col=utils.get_lag_ar1d(sta_series, lag)
    # LZN MARK
    return(0,0)

def parse_line(line, df):
    '''Let's parse the individual line'''
    try:
        year=int(line[0:4])
    except:
        return(df)

    line0=line[4:]
    for ii in range(0, 12):
        date0=datetime.date(year,ii+1,1)
        value0=float(line0[7*ii:7*ii+7])
        if value0 > -999.0:
            df.loc[date0]=value0
    return(df)


def prepare_qbo(qbo_dir, date_series):
    '''deal with the original qbo data'''
    
    df = pd.DataFrame(np.nan, index=date_series, columns=['qbo'])
    
    
    with open (qbo_dir+'qbo.u50.ano.index', 'r') as fr:
        lines=fr.readlines()
        for line in lines:
            df = parse_line(line, df)
    df.to_csv(qbo_dir+'qbo.u50.index.csv')

def prepare_cpc(cpc_prim_lib_dir, date_series):
    
    cpc_prim_lib=['detrend.nino34.ascii.txt', 'monthly.aao.index.b79.current.ascii',
                'nao_index.tim', 'pna_index.tim', 'qbo.u50.index.csv', 
                'epnp_index.tim', 'monthly.ao.index.b50.current.ascii',
                'poleur_index.tim',  'wp_index.tim']

    #dict map for loading cpc idx

    # init np array
    np_idx_array=np.zeros((len(cpc_prim_lib),len(date_series)))
    icount=0
    for itm in cpc_prim_lib:
        if itm=='detrend.nino34.ascii.txt':
            np_cpc_prim =load_nino(cpc_prim_lib_dir, itm, date_series[0].year)
        elif itm=='monthly.aao.index.b79.current.ascii':        
            np_cpc_prim =load_cpc_idx(cpc_prim_lib_dir, itm, date_series[0].year,None,3)
        elif itm=='monthly.ao.index.b50.current.ascii':        
            np_cpc_prim =load_cpc_idx(cpc_prim_lib_dir, itm, date_series[0].year,None,3)
        elif itm=='qbo.u50.index.csv':        
            df_cpc_prim=pd.read_csv(cpc_prim_lib_dir+itm)
            np_cpc_prim=df_cpc_prim['qbo'].values
        else:
            np_cpc_prim =load_cpc_idx(cpc_prim_lib_dir, itm, date_series[0].year,9,3)
        np_idx_array[icount,:]=np_cpc_prim
        icount=icount+1
    
    np_idx_array=np.where(abs(np_idx_array)>90,0.0,np_idx_array)
    np_idx_array=np.transpose(np_idx_array)
    np_idx_array=(np_idx_array-np_idx_array.mean(axis=0))/np_idx_array.std(axis=0)
    df =pd.DataFrame(np_idx_array, index=date_series, columns=['nino34','aao','nao','pna','qbo','epnp','ao','poleur','wp'])
    df.to_csv(cpc_prim_lib_dir+'all_org_features.csv')
    
def load_nino(path_cpc_idx, cpc_lib, strt_yr):
    '''load nino idx'''
    df_cpc_idx_raw=pd.read_csv(path_cpc_idx+cpc_lib, sep='\s+')
    df_cpc_idx_raw=df_cpc_idx_raw[(df_cpc_idx_raw['YR']>=strt_yr)]
    return(df_cpc_idx_raw['ANOM'].values)

def load_cpc_idx(path_cpc_idx, cpc_lib, strt_yr, strt_line, data_col):
    '''dict map for loading cpc idx'''
    df_cpc_idx_raw=pd.read_csv(path_cpc_idx+cpc_lib, sep='\s+', header=strt_line)
    df_cpc_idx_raw=df_cpc_idx_raw[(df_cpc_idx_raw.iloc[:,0]>=strt_yr)]
    return df_cpc_idx_raw.iloc[:,2].values


