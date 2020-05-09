import pytest
from logger import logger
from ..manager.zip_manager import load_dependencies

def test_download_from_s3():
    zip_filepath = '/tmp/package.zip'
    load_dependencies(zip_filepath)