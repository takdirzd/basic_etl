from etl import *



file_path = "E:/DATA/ALGORITMA/.DATA ENGINEERING/BASIC ETL/basic-etl-pipeline-main/data/companies.csv"
save_path = "E:/DATA/ALGORITMA/.DATA ENGINEERING/BASIC ETL/basic-etl-pipeline-main/data/companies_clean.csv"

def run_pipeline(file_path:str, save_path:str):

    # extract
    df = extract(file_path=file_path)

    # transform
    df = transform(df=df)

    # load
    load(df=df, save_path=save_path)

    return


if __name__ == "__main__":
    # run pipeline
    run_pipeline(file_path=file_path, save_path=save_path)