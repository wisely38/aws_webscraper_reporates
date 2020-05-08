from fastavro import writer, reader, parse_schema
import json
from logger import logger

def get_avro_schema(schem_filename):
    with open(schem_filename, "r") as rh:
        return json.load(rh)

def convert_to_avro(df, avro_filename, avro_schema):
    try:
        schema = get_avro_schema(avro_schema)
    except Exception as e:
        logger.error("ERROR - fail to read AVRO schema - %s"%avro_schema)
        logger.error(e)
        raise e
    try:
        parsed_schema = parse_schema(schema)
    except Exception as e:
        logger.error("ERROR - fail to parse AVRO schema - %s"%avro_schema)
        logger.error(e)
        raise e
    logger.info("INFO - start writing AVRO file - %s"%avro_filename)
    with open(avro_filename, 'wb') as (out, err):
        if err:
            logger.error("ERROR - fail to write AVRO file - %s"%avro_filename)
            raise err
        else:
            writer(out, parsed_schema, df.to_dict(orient='records'))
            logger.info("INFO - end writing AVRO file - %s"%avro_filename)