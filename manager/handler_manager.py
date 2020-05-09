from logger import logger
# from string import Template
from jinja2 import Template
import json
import re
from collections import defaultdict

def recover_string_template(cfg_obj, field):
    template_str = cfg_obj[field]
    placeholders = re.findall(r"\{\{([^\}]+)\}\}", template_str)
    rendering_params = defaultdict()
    for placeholder in placeholders:
        rendering_params[placeholder]=cfg_obj[placeholder]
    t = Template(template_str)
    return t.render(rendering_params)    


def cfg_loader(cfg_filename, is_datasource_cfg=False):
    try:
        logger.info("INFO - start loading config - %s"%cfg_filename)            
        with open(cfg_filename, "r") as rh:
            config_obj = json.load(rh)
            if is_datasource_cfg:
                config_obj['url'] = recover_string_template(config_obj, "url_template")
                config_obj['xls_filename'] = recover_string_template(config_obj, "xls_filename_template")
                config_obj['avro_filename'] = recover_string_template(config_obj, "avro_filename_template")
        logger.info("INFO - end loading config - %s"%cfg_filename)
        return config_obj
    except Exception as err:
        logger.error("ERROR - fail to load handler config - %s"%cfg_filename)
        logger.error(err)
        raise err

            
