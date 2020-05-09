from logger import logger

def write_to_excel(response, xls_filename):
    try:
        logger.info("INFO - start writing XLS file - %s"%xls_filename)
        with open(xls_filename, "wb") as wh:
            wh.write(response.content)
        logger.info("INFO - end writing XLS file - %s"%xls_filename)
    except Exception as e:
        logger.error("ERROR - fail to write XLS file - %s"%xls_filename)
        logger.error(e)
        raise e
    return xls_filename