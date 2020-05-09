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
            config_obj = self.load_cfg(cfg_path)
            self.cfgobj.update(config_obj)
        data_cfg_path = os.path.join(self.cfgobj["config_path"], self.cfgobj["source_config_filename"])            
        config_obj = self.load_cfg(data_cfg_path, is_datasource_cfg = True)
        self.cfgobj.update(config_obj)
        self.load_eventobj(eventobj, self.cfgobj)

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
        current_date_str = current_dateobj.strftime("%Y-%m-%d")
        start_date_str = start_dateobj.strftime("%Y-%m-%d")
        startdate = cfg_obj['startdate'] if 'startdate' in cfg_obj else eventobj['startdate'] if  'startdate' in eventobj else start_date_str
        enddate = cfg_obj['enddate'] if 'enddate' in cfg_obj else eventobj['enddate'] if 'enddate' in eventobj else current_date_str
        cfg_obj['startdate'] = startdate
        cfg_obj['enddate'] = enddate
        return cfg_obj

    def load_cfg(self, cfg_filename, is_datasource_cfg = False):
        new_cfgobj = self.load_cfgfile(cfg_filename)
        if is_datasource_cfg:
            new_cfgobj['url'] = self.recover_string_template(new_cfgobj, "url_template")
            new_cfgobj['xls_filename'] = self.recover_string_template(new_cfgobj, "xls_filename_template")
            new_cfgobj['avro_filename'] = self.recover_string_template(new_cfgobj, "avro_filename_template")
        return new_cfgobj      

    def get_cfgobj(self):
        return self.cfgobj

    def load_cfgfile(self, cfg_filename):        
        try:
            logger.info("INFO - start loading config - %s"%cfg_filename)            
            with open(cfg_filename, "r") as rh:
                config_obj = json.load(rh)
            logger.info("INFO - end loading config - %s"%cfg_filename)
            return config_obj
        except Exception as err:
            logger.error("ERROR - fail to load handler config - %s"%cfg_filename)
            logger.error(err)
            raise err

            