from logger  import logger
import json
import requests
import os
from manager.dataframe_manager import read_dataframe
from manager.excel_manager import write_to_excel
from manager.avro_manager import convert_to_avro
from manager.handler_manager import cfg_loader

def scrape_repo_sofr(event, context):
    function_response_code = 200
    handler_cfg = cfg_loader("handler_cfg.json")
    source_cfg_path = os.path.join(handler_cfg["config_path"], handler_cfg["source_config_filename"])
    source_cfg = cfg_loader(source_cfg_path,is_datasource_cfg=True)
    url = source_cfg['url']    
    startdate = source_cfg['startdate']
    enddate = source_cfg['enddate']
    logger.info("INFO - fetching data between (%s,%s)"%(startdate, enddate))
    # url = "https://websvcgatewayx2.frbny.org/mktrates_external_httponly/services/v1_0/mktRates/excel/retrieve?multipleRateTypes=true&startdate=%(startdate)s&enddate=%(enddate)s&rateType=R3"%{"startdate":startdate,"enddate":enddate}
    session = requests.session()
    logger.info("INFO - fetching url - %s"%url)
    result = session.get(url)
    if result.status_code >= 200 and result.status_code < 300:
        xls_filename = os.path.join(handler_cfg["temp_path"], source_cfg['xls_filename'])
        avro_filename =os.path.join(handler_cfg["output_path"],  source_cfg['avro_filename'])
        avro_schema = os.path.join(handler_cfg["schema_path"],source_cfg['avro_schema'])
        header_linenumber = source_cfg['header_linenumber']
        logger.info("INFO - writing excel from url - %s"%url)
        write_to_excel(result, xls_filename)
        logger.info("INFO - writing dataframe from url - %s"%url)
        repo_df = read_dataframe(xls_filename, header_linenumber)
        logger.info("INFO - writing AVRO from url - %s"%url)
        convert_to_avro(repo_df, avro_filename, avro_schema)        
    else:
        logger.error("ERROR - failed to fetch url, error code - %s"%result.status_code)
        function_response_code = result.status_code
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
