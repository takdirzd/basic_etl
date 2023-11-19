import pandas as pd

from pandas.core.frame import DataFrame

# Extract

def extract(file_path: str) -> DataFrame:

    df = pd.read_csv(file_path)

    return (df)

tes = extract("E:/DATA/ALGORITMA/.DATA ENGINEERING/BASIC ETL/basic-etl-pipeline-main/data/companies.csv")

# TRANSFORM

def transform(df:DataFrame) -> DataFrame:

    # Drop missing value
    df_clean = df.dropna()

    # Clean decimal in COlumn Year, and change to string

    df_clean.Year = df_clean.Year.astype('int').astype("str")

    return df_clean

df = transform(tes)

def load(df: DataFrame, save_path: str):

    # write dataframe to csv
    df.to_csv(save_path, index=False)
    return

load(df, "E:/DATA/ALGORITMA/.DATA ENGINEERING/BASIC ETL/basic-etl-pipeline-main/data/companies_clean.csv")
