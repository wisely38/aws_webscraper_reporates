from logger import logger

def write_to_excel(response, xls_filepath):
    try:
        logger.info("INFO - start writing XLS file - %s"%xls_filepath)
        with open(xls_filepath, "wb") as wh:
            wh.write(response.content)
        logger.info("INFO - end writing XLS file - %s"%xls_filepath)
    except Exception as e:
        logger.error("ERROR - fail to write XLS file - %s"%xls_filepath)
        logger.error(e)
        raise e