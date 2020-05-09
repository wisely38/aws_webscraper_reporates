from logger import logger
from jinja2 import Template
import json
import re
from collections import defaultdict
import os
from manager.event_manager import parse_event_date
from manager.s3_manager import download_from_s3

class ConfigManager:
    def __init__(self, eventobj, is_runningon_s3=False):
        self.cfgobj = defaultdict()
        if is_runningon_s3:
            s3_main_config_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/config/s3_handler_cfg.json"
            tmp_main_config_path = "/tmp/handler_cfg.json"
            if os.path.exists(tmp_main_config_path):
                os.remove(tmp_main_config_path)
            logger.info('INFO - start copying s3 config - %s'%s3_main_config_path)
            download_from_s3(s3_main_config_path, tmp_main_config_path)
            general_cfg_path = tmp_main_config_path
            logger.info('INFO - end copying s3 config - %s'%s3_main_config_path)
        else:
            local_main_config_path = "handler_cfg.json"
            logger.info('INFO - start copying local config - %s'%local_main_config_path)
            general_cfg_path = local_main_config_path
            logger.info('INFO - end copying local config - %s'%local_main_config_path)            

        try:
            logger.info("INFO - start loading main config - %s"%general_cfg_path)
            config_obj = self.load_cfg(general_cfg_path)
            self.cfgobj.update(config_obj)
            logger.info("INFO - end loading main config - %s"%general_cfg_path)
        except Exception as err:
            logger.error("ERROR - fail to load main config - %s"%general_cfg_path)
            logger.error(err)
            raise err

        if is_runningon_s3:
            self.prepare_tmp_paths(self.cfgobj)
            self.copy_configs(self.cfgobj)

        try:
            data_cfg_path = os.path.join(self.cfgobj["data_config_path"], self.cfgobj["source_config_filename"])
            logger.info("INFO - start loading data config - %s"%data_cfg_path)
            config_obj = self.load_cfg(data_cfg_path, is_datasource_cfg = True)
            self.cfgobj.update(config_obj)
            logger.info("INFO - end loading data  config - %s"%data_cfg_path)
        except Exception as err:
            logger.error("ERROR - fail to load data config - %s"%data_cfg_path)
            logger.error(err)
            raise err

        try:            
            logger.info("INFO - start loading event config - %s"%eventobj)
            self.load_eventobj(eventobj, self.cfgobj)
            logger.info("INFO - end loading event config - %s"%eventobj)
        except Exception as err:
            logger.error("ERROR - fail to load event datetime - %s"%eventobj)
            logger.error(err)
            raise err
            
        self.cfgobj['url'] = self.recover_string_template(self.cfgobj, "url_template")
        self.cfgobj['xls_filename'] = self.recover_string_template(self.cfgobj, "xls_filename_template")
        self.cfgobj['avro_filename'] = self.recover_string_template(self.cfgobj, "avro_filename_template")


    def copy_configs(self, cfg):    
        s3_data_config_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/config/s3_source_cfg.json"
        tmp_data_config_path = os.path.join(cfg["data_config_path"], "source_cfg.json")
        if os.path.exists(tmp_data_config_path):
            os.remove(tmp_data_config_path)
        logger.info('INFO - start copying data config - %s to %s '%(s3_data_config_path,tmp_data_config_path))
        download_from_s3(s3_data_config_path, tmp_data_config_path)
        logger.info('INFO - end copying data config - %s to %s '%(s3_data_config_path,tmp_data_config_path))

        s3_data_schema_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/schema/s3_sofr_schema.json"
        tmp_data_schema_path = os.path.join(cfg["schema_path"], "sofr_schema.json")
        if os.path.exists(tmp_data_schema_path):
            os.remove(tmp_data_schema_path)
        logger.info('INFO - start copying data schema - %s to %s '%(s3_data_schema_path,tmp_data_schema_path))
        download_from_s3(s3_data_schema_path, tmp_data_schema_path)
        logger.info('INFO - end copying data schema - %s to %s '%(s3_data_schema_path,tmp_data_schema_path))


    def prepare_tmp_paths(self, cfg):
        if not os.path.isdir(cfg["temp_path"]):
            os.mkdir(cfg["temp_path"])
        if not os.path.isdir(cfg["output_path"]):            
            os.mkdir(cfg["output_path"])
        if not os.path.isdir(cfg["data_config_path"]):                        
            os.mkdir(cfg["data_config_path"])
        if not os.path.isdir(cfg["schema_path"]):                        
            os.mkdir(cfg["schema_path"])

    def recover_string_template(self, cfg_obj, field):
        template_str = cfg_obj[field]
        placeholders = re.findall(r"\{\{([^\}]+)\}\}", template_str)
        rendering_params = defaultdict()
        for placeholder in placeholders:
            rendering_params[placeholder]=cfg_obj[placeholder]
        t = Template(template_str)
        return t.render(rendering_params)    

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

            
