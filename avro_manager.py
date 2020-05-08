from fastavro import writer, reader, parse_schema
import json
from logger import logger

def get_avro_schema():
    with open("sofr_schema.json", "r") as rh:
        return json.load(rh)

def convert_to_avro(df, avro_filename):
    schema = get_avro_schema()
    parsed_schema = parse_schema(schema)
    with open(avro_filename, 'wb') as out:
        writer(out, parsed_schema, df.to_dict(orient='records'))