from logger import logger
from jinja2 import Template
from collections import defaultdict
import re


def recover_string_template(cfg_obj, field):
    template_str = cfg_obj[field]
    placeholders = re.findall(r"\{\{([^\}]+)\}\}", template_str)
    rendering_params = defaultdict()
    for placeholder in placeholders:
        rendering_params[placeholder]=cfg_obj[placeholder]
    t = Template(template_str)
    return t.render(rendering_params)    