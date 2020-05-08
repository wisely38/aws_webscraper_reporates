import re
import pandas as pd
import locale
from locale import atof
from logger import logger


def cleanse_header(heading):
    return re.sub(r"\([^\)]+\)", "", heading).replace("\n","").strip()

def cleanse_datefield(datefield):
    return re.sub(r"\[.+\]", "", str(datefield).strip())

def convert_to_timestamp(series, date_str):
    return pd.to_datetime(series, format=date_str)

def read_dataframe(xls_filename):
    logger.info("INFO - start reading XLS file - %s"%xls_filename)
    locale.setlocale(locale.LC_NUMERIC, '')
    repo_df = pd.read_excel(xls_filename, header=3)
    raw_columns = repo_df.columns
    repo_df.columns = list(map(cleanse_header, raw_columns))
    repo_df['DATE'] = repo_df['DATE'].apply(cleanse_datefield)
    repo_df = repo_df[repo_df['DATE'].str.len()==10]
    repo_df['DATE'] = convert_to_timestamp(repo_df['DATE'], '%Y-%m-%d')
    repo_df['RATE'] = repo_df['RATE'].astype("float64")
    repo_df['1ST'] = repo_df['1ST'].astype("float64")
    repo_df['25TH'] = repo_df['25TH'].astype("float64")
    repo_df['75TH'] = repo_df['75TH'].astype("float64")
    repo_df['99TH'] = repo_df['99TH'].astype("float64")
    repo_df['VOLUME'] = repo_df['VOLUME'].apply(atof)
    repo_df['VOLUME'] = repo_df['VOLUME'].astype("int64")
    return repo_df