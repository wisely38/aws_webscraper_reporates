import sys
import os
from manager.zip_manager import load_dependencies
from manager.s3_manager import download_from_s3, upload_to_s3

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
from manager.config_manager import ConfigManager

def scrape_repo_sofr(event, context):
    logger.info('INFO - start running function - scrape_repo_sofr')
    function_response_code = 200
    if context.get_remaining_time_in_millis() > 0:
        s3_config_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/config/s3_handler_cfg.json"
        config_path = "/tmp/handler_cfg.json"
        logger.info('INFO - start loading s3 config - %s'%s3_config_path)
        download_from_s3(s3_config_path, config_path)
        cfg = ConfigManager([config_path], event).get_cfgobj()
        logger.info('INFO - end loading s3 config - %s'%s3_config_path)
    else:
        local_config_path = "handler_cfg.json"
        logger.info('INFO - start loading local config - %s'%local_config_path)
        cfg = ConfigManager([local_config_path], event).get_cfgobj()
        logger.info('INFO - end loading local config - %s'%local_config_path)
    url = cfg['url']
    date_format = cfg['date_format'] if cfg['date_format'] else '%Y-%m-%d'
    logger.info("INFO - fetching data between (%s,%s)"%(cfg['startdate'], cfg['enddate']))
    session = requests.session()
    logger.info("INFO - fetching url - %s"%url)
    result = session.get(url)
    if result.status_code >= 200 and result.status_code < 300:
        os.mkdir(cfg["temp_path"])
        os.mkdir(cfg["output_path"])
        xls_filename = os.path.join(cfg["temp_path"], cfg['xls_filename'])
        avro_filename =os.path.join(cfg["output_path"],  cfg['avro_filename'])
        avro_schema = os.path.join(cfg["schema_path"],cfg['avro_schema'])
        header_linenumber = cfg['header_linenumber']
        logger.info("INFO - writing excel from url - %s"%url)
        write_to_excel(result, xls_filename)
        logger.info("INFO - writing dataframe from url - %s"%url)
        repo_df = read_dataframe(xls_filename, date_format, header_linenumber)
        logger.info("INFO - writing AVRO from url - %s"%url)
        convert_to_avro(repo_df, avro_filename, avro_schema)
        upload_to_s3(avro_filename, cfg['avro_filename'], cfg['year'], cfg['month'])
    else:
        logger.error("ERROR - failed to fetch url, error code - %s"%result.status_code)
        function_response_code = result.status_code
    logger.info('INFO - function remaining time: %s'%context.get_remaining_time_in_millis())
    logger.info('INFO - end running function - scrape_repo_sofr')     
    body = {
        "message": "Repo SOFR function executed successfully!",
        "input": event
    }
    response = {
        "statusCode": function_response_code,
        "body": json.dumps(body)
    }
    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "event": event
    }
    """
