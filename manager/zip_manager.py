import zipfile
import urllib
import os
from logger import logger

def load_dependencies(zip_filepath):
    folder_parsed = urllib.parse.urlparse(zip_filepath)
    zip_folder = os.path.split(folder_parsed.path)[0]
    try:
        logger.info("INFO - start unzipping file - %s to folder %s"%(zip_filepath,zip_folder))
        with zipfile.ZipFile(zip_filepath, 'r') as zh:
            zh.extractall(zip_folder)
        logger.info("INFO - end unzipping file - %s to folder %s"%(zip_filepath,zip_folder))
    except Exception as err:
        logger.error("ERROR - fail to unzip file - %s to folder %s"%(zip_filepath,zip_folder))
        logger.error(err)
        raise err            