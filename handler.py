from logger  import logger
import json
import requests
import os
from manager.dataframe_manager import read_dataframe
from manager.excel_manager import write_to_excel
from manager.avro_manager import convert_to_avro
from manager.config_manager import ConfigManager
import datetime

def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

def scrape_repo_sofr(event, context):
    logger.info('INFO - start running function: %s'%context.functionName)
    function_response_code = 200
    cfg = ConfigManager(["handler_cfg.json"],event).get_cfgobj()
    url = cfg['url']
    date_format = cfg['date_format'] if cfg['date_format'] else '%Y-%m-%d'
    logger.info("INFO - fetching data between (%s,%s)"%(cfg['startdate'], cfg['enddate']))
    session = requests.session()
    logger.info("INFO - fetching url - %s"%url)
    result = session.get(url)
    if result.status_code >= 200 and result.status_code < 300:
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
    else:
        logger.error("ERROR - failed to fetch url, error code - %s"%result.status_code)
        function_response_code = result.status_code
    logger.info('INFO - function remaining time: %s'%context.getRemainingTimeInMillis())
    logger.info('INFO - end running function: %s'%context.functionName)        
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
