#/usr/bin/env python3
'''
Date: Feb 14, 2021

This is the main script to drive the model in routine run.

Zhenning LI
'''

import os, logging
import pandas as pd
import datetime
import lib.time_manager
import lib.etl_manager
from lib.cfgparser import read_cfg
from utils import utils

def main_run():
    """ main script """
    
    time_mgr=lib.time_manager.time_manager()
    
    # logging manager
    logging.config.fileConfig('./conf/logging_config.ini')
    
    utils.write_log('*************************SPELLCASTER-OCULUS**************************')
    
    utils.write_log('>>Read Config...')
    cfg_hdl=read_cfg('./conf/config.ini')
    
    etl_mgr=lib.etl_manager.etl_manager(cfg_hdl)

   
    utils.write_log('>>Prepare raw data...')
   
    # NCC indicies
    if cfg_hdl['PRELOAD_OPT'].getboolean('exe_down_ncc'):
        os.system('sh ./preload/down_ncc.sh '+etl_mgr.ncc_path)
    # cpc index
    if cfg_hdl['PRELOAD_OPT'].getboolean('exe_down_cpc'):
        os.system('sh ./preload/down_cpc.sh '+cpc_path)
    # reanalysis data
    if cfg_hdl['PRELOAD_OPT'].getboolean('exe_down_ra'):
        down_ra(cfg_hdl)
    # dynamic forecast
    if cfg_hdl['PRELOAD_OPT'].getboolean('exe_down_dyn'):
        os.system('sh ./preload/down_cfsv2.sh '+cfs_path)

    time_mgr.toc('REANALYSIS DOWNLOAD')


    utils.write_log('>>ETL from raw data...')
    
    if cfg_hdl['PRELOAD_OPT'].getboolean('exe_compute_ncc'):
        etl_mgr.etl_ncc()
    
    if cfg_hdl['PRELOAD_OPT'].getboolean('exe_compute_ts'):
        etl_mgr.etl_giss()
        
    if cfg_hdl['PRELOAD_OPT'].getboolean('exe_compute_pr'):
        etl_mgr.etl_prec()

    if cfg_hdl['CORE'].getboolean('build_ts_model'):
        etl_mgr.load_giss()
        sum_short=0
        for sta in etl_mgr.sta_lst:
            (X,Y)=lib.etl_manager.gen_one_staXY_ts(etl_mgr,str(sta.sta_num))
            sum_short=sum_short+Y
        print(sum_short)
    time_mgr.dump()
    utils.write_log('*************************SPELLCASTER-OCULUS**************************')

def down_ra(cfg_hdl):
    """ Download Reanalysis Data """
    outpath=cfg_hdl['INPUT_OPT']['oculus_root']+cfg_hdl['INPUT_OPT']['ra_dir']
    ts_name=cfg_hdl['INPUT_OPT']['ra_ts_name']
    hgt_name=cfg_hdl['INPUT_OPT']['ra_hgt_name']
    pr_name=cfg_hdl['INPUT_OPT']['ra_pr_name']

    os.system('sh ./preload/down_reanalysis.sh '+outpath+' '+ts_name+' '+hgt_name+' '+pr_name)

if __name__=='__main__':
    main_run()
