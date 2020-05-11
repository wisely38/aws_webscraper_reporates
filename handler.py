import sys
import os
from manager.zip_manager import load_dependencies
from manager.s3_manager import download_from_s3, upload_to_s3_repo_sofr

download_path = "/tmp/data-collector-repo-sofr-app.zip"
s3_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/deployment/data-collector-repo-sofr-app.zip"
download_from_s3(s3_path, download_path)
load_dependencies(download_path)
# sys.path.insert(0, "/tmp/data-collector-repo-sofr-app")
# user_home = os.environ["HOME"]
os.environ["PYTHONPATH"] = "/tmp/package:./"
sys.path.append("/tmp/package")


from logger  import logger
import json
import requests
import os
from manager.dataframe_manager import read_dataframe
from manager.excel_manager import write_to_excel
from manager.avro_manager import convert_to_avro
# from manager.config_manager import ConfigManager
from manager.base_config_manager import BaseConfigManager
from manager.template_helper import recover_string_template
from manager.crawl_manager import Crawler

# temp_folder = "/tmp"
# source_maincfg_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/config/s3_main_cfg.json"
# source_datacfg_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/config/s3_data_cfg.json"
# source_schema_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/schema/s3_sofr_schema.json"
# dest_maincfg_filename = "main_cfg.json"
# dest_datacfg_filename = "data_cfg.json"
# dest_schema_filename = "sofr_schema.json"

def do_build_http_response(event, function_response_code):
    body = {
        "message": "Repo SOFR function executed successfully!",
        "input": event
    }
    response = {
        "statusCode": function_response_code,
        "body": json.dumps(body)
    }
    return response

def do_fetch_error_handlng(function_response_code):
    fetch_error_msg =  "ERROR - failed to fetch url, error code - %s"%function_response_code
    logger.error(fetch_error_msg)
    raise Exception(fetch_error_msg)

def do_fetching_data(url):
    crawler = Crawler(url)
    return crawler.get_crawled_result(), crawler.is_crawled_result_valid(), crawler.get_crawled_result().status_code

def do_generate_output(df, temp_folder, cfg, url):
    logger.info("INFO - writing AVRO from url - %s"%url)
    cfg['startdate'] = df['PERIOD_START'].values[0].replace("-","")
    cfg['enddate'] = df['PERIOD_END'].values[0].replace("-","")
    cfg['avro_filename'] = recover_string_template(cfg, "avro_filename_template")
    avro_filepath =os.path.join(temp_folder, cfg["output_path"],  cfg['avro_filename'])
    avro_schema = os.path.join(temp_folder, cfg["schema_path"],cfg['avro_schema'])
    convert_to_avro(df, avro_filepath, avro_schema)
    bucket_name = cfg['s3_output_bucket_avro']
    key_prefix = cfg['s3_output_key_prefix']     
    upload_to_s3_repo_sofr(avro_filepath, cfg['avro_filename'], bucket_name, key_prefix, cfg['year'], cfg['month'])

def do_data_processing(fetch_result, temp_folder, cfg, url):
    date_format = cfg['date_format'] if cfg['date_format'] else '%Y-%m-%d'
    header_linenumber = cfg['header_linenumber']
    logger.info("INFO - writing excel from url - %s"%url)
    xls_filepath = os.path.join(temp_folder, cfg["temp_path"], cfg['xls_filename'])
    write_to_excel(fetch_result, xls_filepath)
    logger.info("INFO - writing dataframe from url - %s"%url)
    return read_dataframe(xls_filepath, date_format, header_linenumber)

def load_handler_cfg(is_running_on_s3):
    if is_running_on_s3:
        with open("config/s3_handler_cfg.json") as rh:
            handler_cfg = json.load(rh)
    else:
        with open("config/handler_cfg.json") as rh:
            handler_cfg = json.load(rh)
    handler_cfg['source_maincfg_path'] = os.path.join(handler_cfg['cfg_basepath'], handler_cfg['source_maincfg_contextpath'])
    handler_cfg['source_datacfg_path'] = os.path.join(handler_cfg['cfg_basepath'], handler_cfg['source_datacfg_contextpath'])
    handler_cfg['source_schema_path'] = os.path.join(handler_cfg['cfg_basepath'], handler_cfg['source_schema_contextpath'])
    return handler_cfg

def scrape_repo_sofr(event, context):
    logger.info('INFO - start running function - scrape_repo_sofr')
    # cfg = ConfigManager(event, hasattr(context,"function_name")).get_cfgobj()
    is_running_on_s3 = hasattr(context, "function_name")
    handler_cfg = load_handler_cfg(is_running_on_s3)
    temp_folder = handler_cfg['temp_folder']
    cfg = BaseConfigManager(event, handler_cfg, is_running_on_s3).get_cfgobj()

    url = cfg['url']
    logger.info("INFO - trying to fetch data between (%s,%s)"%(cfg['startdate'], cfg['enddate']))
    fetch_result, is_result_valid, function_response_code = do_fetching_data(url)

    if is_result_valid:        
        df = do_data_processing(fetch_result, temp_folder, cfg, url)
        do_generate_output(df, temp_folder, cfg, url)
    else:
        do_fetch_error_handlng(function_response_code)

    logger.info('INFO - function remaining time: %s'%context.get_remaining_time_in_millis())
    logger.info('INFO - end running function - scrape_repo_sofr')     
    return do_build_http_response(event, function_response_code)
