import pytest
from logger import logger
from ..manager.s3_manager import download_from_s3, upload_to_s3_repo_sofr

@pytest.mark.skip(reason="temp")
def test_download_from_s3():
    download_path = "/tmp/package.zip"
    s3_path = "s3://eternity02.deployment/lambda/data-collector-repo-sofr-app/deployment/package.zip"
    download_from_s3(s3_path, download_path)

@pytest.mark.skip(reason="temp")
def test_upload_to_s3():
    upload_path = 'data-collector-repo-sofr-app.zip'
    filename = 'data-collector-repo-sofr-app.zip'
    upload_to_s3_repo_sofr(upload_path, filename)