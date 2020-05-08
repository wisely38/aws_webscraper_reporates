from logger  import logger
import json
import requests
from dataframe_manager import read_dataframe
from excel_manager import write_to_excel
from avro_manager import convert_to_avro

def scrape_repo_sofr(event, context):
    function_response_code = 200
    startdate = '04022018'
    enddate = '03242020'
    logger.info("INFO - fetching data between (%s,%s)"%(startdate, enddate))
    url = "https://websvcgatewayx2.frbny.org/mktrates_external_httponly/services/v1_0/mktRates/excel/retrieve?multipleRateTypes=true&startdate=%(startdate)s&enddate=%(enddate)s&rateType=R3"%{"startdate":startdate,"enddate":enddate}
    session = requests.session()
    logger.info("INFO - fetching url - %s"%url)
    result = session.get(url)
    if result.status_code >= 200 and result.status_code < 300:
        xls_filename = 'repo-sofr_%(startdate)s_%(enddate)s.xls'%{"startdate":startdate,"enddate":enddate}
        avro_filename = 'repo-sofr_%(startdate)s_%(enddate)s.avro'%{"startdate":startdate,"enddate":enddate}
        avro_schema = "sofr_schema.json"
        logger.info("INFO - writing excel from url - %s"%url)
        write_to_excel(result, xls_filename)
        logger.info("INFO - writing dataframe from url - %s"%url)
        repo_df = read_dataframe(xls_filename, 3)
        logger.info("INFO - writing AVRO from url - %s"%url)
        convert_to_avro(repo_df, avro_filename, avro_schema)        
    else:
        logger.error("ERROR - failed to fetch url, error code - %s"%result.status_code)
        function_response_code = result.status_code
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
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
