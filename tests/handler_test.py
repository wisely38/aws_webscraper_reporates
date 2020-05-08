# import logging
import pytest
from ..logger import logger

from..handler import scrape_repo_sofr
# from ..dataframe_manager import read_dataframe
# from ..avro_manager import convert_to_avro
# import sys, os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dataframe_manager import read_dataframe
from avro_manager import convert_to_avro

# import read_dataframe
# import convert_to_avro

# logging.basicConfig(filename="handler_test.log",level=logging.INFO)
# logging.basicConfig(stream=sys.stdout, level=logging.INFO)
# logger = logging.getLogger()


# def test_func_fast():
#     event = {"key": "value"}
#     context = {}
#     response = scrape_repo_sofr(event, context)
#     print(response)


# def test_read_dataframe():
#     xls_filename='repo-sofr_04022018_03242020.xls'
#     logger.info("file:%s"%xls_filename)
#     repo_df = read_dataframe(xls_filename)
#     logger.info(repo_df.columns)

def test_convert_to_avro():
    xls_filename = 'repo-sofr_04022018_03242020.xls'
    avro_filename='repo-sofr_04022018_03242020.avro'
    repo_df = read_dataframe(xls_filename, 3)
    convert_to_avro(repo_df,avro_filename)


    
    