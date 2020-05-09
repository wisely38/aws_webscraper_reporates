from logger import logger
from jinja2 import Template
import json
import re
from collections import defaultdict
import os
from manager.event_manager import parse_event_date

class ConfigManager:
    def __init__(self, general_cfg_paths, eventobj):
        self.cfgobj = defaultdict()
        for cfg_path in general_cfg_paths:
            try:
                config_obj = self.load_cfg(cfg_path)
                self.cfgobj.update(config_obj)
            except Exception as err:
                logger.error("ERROR - fail to load config - %s"%cfg_path)
                logger.error(err)
        data_cfg_path = os.path.join(self.cfgobj["config_path"], self.cfgobj["source_config_filename"])
        try:         
            config_obj = self.load_cfg(data_cfg_path, is_datasource_cfg = True)
            self.cfgobj.update(config_obj)
        except Exception as err:
            logger.error("ERROR - fail to load config - %s"%data_cfg_path)
            logger.error(err)
            raise err
        try:            
            self.load_eventobj(eventobj, self.cfgobj)
        except Exception as err:
            logger.error("ERROR - fail to load event datetime - %s"%eventobj)
            logger.error(err)
            raise err
        self.cfgobj['url'] = self.recover_string_template(self.cfgobj, "url_template")
        self.cfgobj['xls_filename'] = self.recover_string_template(self.cfgobj, "xls_filename_template")
        self.cfgobj['avro_filename'] = self.recover_string_template(self.cfgobj, "avro_filename_template")

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

            
