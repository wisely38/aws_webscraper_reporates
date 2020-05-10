# import logging
import pytest
from logger import logger

from handler import scrape_repo_sofr
# from ..dataframe_manager import read_dataframe
# from ..avro_manager import convert_to_avro
# import sys, os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ..manager.dataframe_manager import read_dataframe
from ..manager.avro_manager import convert_to_avro
from ..manager.config_manager import ConfigManager
import requests


# @pytest.mark.skip(reason="temp")
def test_func_fast():
    # event = {
    #     "startdate": "04022017",
    #     "enddate": "05092020",
    #     "source": "aws.events",
    #     "time": "2019-03-01T01:23:45Z",
    #     "manual_triggered": "true"
    # }
    event = {
        "version": "0",
        "id": "53dc4d37-cffa-4f76-80c9-8b7d4a4d2eaa",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": "2015-10-08T16:53:06Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/my-scheduled-rule"
        ],
        "detail": {}
    }    
    class Context:
        def __init__(self):
            self.function_name = "s3"
            # pass 
        def get_remaining_time_in_millis(self):
            return 1500
    context = Context()
    response = scrape_repo_sofr(event, context)
    print(response)

@pytest.mark.skip(reason="temp")
def test_read_dataframe():
    xls_filename='repo-sofr_04022018_03242020.xls'
    logger.info("file:%s"%xls_filename)
    repo_df = read_dataframe(xls_filename)
    logger.info(repo_df.columns)

@pytest.mark.skip(reason="temp")
def test_convert_to_avro():
    avro_schema = "sofr_schema.json"
    xls_filename = 'repo-sofr_04022018_03242020.xls'
    avro_filename='repo-sofr_04022018_03242020.avro'
    repo_df = read_dataframe(xls_filename, 3)
    convert_to_avro(repo_df, avro_filename, avro_schema)

@pytest.mark.skip(reason="temp")
def test_handler_cfg_loader():
    event = {
        "version": "0",
        "id": "53dc4d37-cffa-4f76-80c9-8b7d4a4d2eaa",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": "2015-10-08T16:53:06Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/my-scheduled-rule"
        ],
        "detail": {}
    }        
    cfg = ConfigManager(event, False).get_cfgobj()
    url = cfg['url']
    session = requests.session()
    logger.info("INFO - fetching url - %s"%url)
    result = session.get(url)
    logger.info("INFO - status code - %s"%result.status_code)


    
    