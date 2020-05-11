from logger import logger
from jinja2 import Template
import json
import re
from collections import defaultdict
import os
from manager.event_manager import parse_event_date
from manager.s3_manager import download_from_s3
from manager.template_helper import recover_string_template

class BaseConfigManager:
    def __init__(self, eventobj, handler_cfg, is_runningon_s3=False):
        self.cfgobj = defaultdict()
        self.source_maincfg_path = handler_cfg['source_maincfg_path']
        self.source_datacfg_path = handler_cfg['source_datacfg_path']
        self.source_schema_path = handler_cfg['source_schema_path']
        self.dest_maincfg_filename = handler_cfg['dest_maincfg_filename']
        self.dest_datacfg_filename = handler_cfg['dest_datacfg_filename']
        self.dest_schema_filename = handler_cfg['dest_schema_filename']
        self.temp_folder = handler_cfg['temp_folder']
        self.is_runningon_s3 = is_runningon_s3
        if is_runningon_s3:
            logger.info('INFO - using s3 main config - %s'%self.source_maincfg_path)
            general_cfg_path = self.do_download_mainconfig(self.source_maincfg_path, self.temp_folder, self.dest_maincfg_filename)
        else:
            logger.info('INFO - using local main config - %s'%self.dest_maincfg_filename)
            general_cfg_path = self.dest_maincfg_filename

        self.do_copy_mainconfig(general_cfg_path, self.cfgobj)
        self.temp_path = self.cfgobj['temp_path']

        if is_runningon_s3:
            self.do_prepare_tmp_paths(self.cfgobj, self.temp_folder)
            self.do_copy_configs(self.cfgobj, self.temp_folder, self.source_datacfg_path, self.source_schema_path, self.dest_datacfg_filename, self.dest_schema_filename)

        self.do_datacfg_processing(self.cfgobj)
        self.do_event_processing(eventobj, self.cfgobj)
        self.do_cfg_postprocessing(self.cfgobj)



    def do_download_mainconfig(self, source_maincfg_path, temp_folder, dest_maincfg_filename):
        tmp_main_config_path = os.path.join(temp_folder, dest_maincfg_filename)
        if os.path.exists(tmp_main_config_path):
            os.remove(tmp_main_config_path)
        logger.info('INFO - start copying s3 main config - %s'%source_maincfg_path)
        download_from_s3(source_maincfg_path, tmp_main_config_path)
        logger.info('INFO - end copying s3 main config - %s'%source_maincfg_path)
        return tmp_main_config_path

    def do_copy_mainconfig(self, maincfg_path, cfg):
        try:
            logger.info("INFO - start loading main config - %s"%maincfg_path)
            config_obj = self.load_cfg(maincfg_path)
            cfg.update(config_obj)
            logger.info("INFO - end loading main config - %s"%maincfg_path)
        except Exception as err:
            logger.error("ERROR - fail to load main config - %s"%maincfg_path)
            logger.error(err)
            raise err
        

    def do_datacfg_processing(self, cfg):
        try:
            data_cfg_path = os.path.join(self.temp_folder, cfg["data_config_path"], cfg["source_config_filename"])
            logger.info("INFO - start loading data config - %s"%data_cfg_path)
            config_obj = self.load_cfg(data_cfg_path, is_datasource_cfg = True)
            cfg.update(config_obj)
            logger.info("INFO - end loading data  config - %s"%data_cfg_path)
        except Exception as err:
            logger.error("ERROR - fail to load data config - %s"%data_cfg_path)
            logger.error(err)
            raise err        

    def do_event_processing(self, eventobj, cfg):
        try:            
            logger.info("INFO - start loading event config - %s"%eventobj)
            self.load_eventobj(eventobj, cfg)
            logger.info("INFO - end loading event config - %s"%eventobj)
        except Exception as err:
            logger.error("ERROR - fail to load event datetime - %s"%eventobj)
            logger.error(err)
            raise err   

    def do_cfg_postprocessing(self, cfg):
        cfg['url'] = recover_string_template(cfg, "url_template")
        cfg['xls_filename'] = recover_string_template(cfg, "xls_filename_template")
        cfg['avro_filename'] = recover_string_template(cfg, "avro_filename_template")

    def do_copy_configs(self, cfg, temp_folder, source_datacfg_path, source_schema_path, dest_datacfg_filename, dest_schema_filename):    
        tmp_data_config_path = os.path.join(temp_folder, cfg["data_config_path"], dest_datacfg_filename)
        if os.path.exists(tmp_data_config_path):
            os.remove(tmp_data_config_path)
        logger.info('INFO - start copying data config - %s to %s '%(source_datacfg_path,tmp_data_config_path))
        download_from_s3(source_datacfg_path, tmp_data_config_path)
        logger.info('INFO - end copying data config - %s to %s '%(source_datacfg_path,tmp_data_config_path))

        tmp_data_schema_path = os.path.join(temp_folder, cfg["schema_path"], dest_schema_filename)
        if os.path.exists(tmp_data_schema_path):
            os.remove(tmp_data_schema_path)
        logger.info('INFO - start copying data schema - %s to %s '%(source_schema_path,tmp_data_schema_path))
        download_from_s3(source_schema_path, tmp_data_schema_path)
        logger.info('INFO - end copying data schema - %s to %s '%(source_schema_path,tmp_data_schema_path))


    def do_prepare_tmp_paths(self, cfg, temp_folder):
        logger.info('INFO - creating folder - %s"'%os.path.join(temp_folder, cfg["temp_path"]))
        if not os.path.isdir(os.path.join(temp_folder, cfg["temp_path"])):
            os.mkdir(os.path.join(temp_folder, cfg["temp_path"]))
        logger.info('INFO - creating folder - %s"'%os.path.join(temp_folder, cfg["output_path"]))
        if not os.path.isdir(os.path.join(temp_folder, cfg["output_path"])):
            os.mkdir(os.path.join(temp_folder, cfg["output_path"]))
        logger.info('INFO - creating folder - %s"'%os.path.join(temp_folder, cfg["data_config_path"]))
        if not os.path.isdir(os.path.join(temp_folder, cfg["data_config_path"])):
            os.mkdir(os.path.join(temp_folder, cfg["data_config_path"]))
        logger.info('INFO - creating folder - %s"'%os.path.join(temp_folder, cfg["schema_path"]))            
        if not os.path.isdir(os.path.join(temp_folder, cfg["schema_path"])):
            os.mkdir(os.path.join(temp_folder, cfg["schema_path"]))

    def load_eventobj(self, eventobj, cfg_obj):
        current_dateobj, start_dateobj = parse_event_date(eventobj)
        current_date_str = current_dateobj.strftime("%m%d%Y")
        start_date_str = start_dateobj.strftime("%m%d%Y")
        startdate = cfg_obj['startdate'] if 'startdate' in cfg_obj else eventobj['startdate'] if  'startdate' in eventobj else start_date_str
        enddate = cfg_obj['enddate'] if 'enddate' in cfg_obj else eventobj['enddate'] if 'enddate' in eventobj else current_date_str
        cfg_obj['startdate'] = startdate
        cfg_obj['enddate'] = enddate
        cfg_obj['year'] = start_dateobj.year
        cfg_obj['month'] = start_dateobj.month
        return cfg_obj

    def load_cfg(self, cfg_filename, is_datasource_cfg = False):
        new_cfgobj = self.load_cfgfile(cfg_filename, is_datasource_cfg)
        return new_cfgobj      

    def get_cfgobj(self):
        return self.cfgobj

    def load_cfgfile(self, cfg_filename, is_datasource_cfg):        
        try:
            logger.info("INFO - start loading config - %s"%cfg_filename)            
            with open(cfg_filename, "r") as rh:
                config_obj = json.load(rh)
            logger.info("INFO - end loading config - %s"%cfg_filename)
            return config_obj
        except Exception as err:
            logger.error("ERROR - fail to load config - %s"%cfg_filename)
            logger.error(err)
            if is_datasource_cfg:
                raise err

            
