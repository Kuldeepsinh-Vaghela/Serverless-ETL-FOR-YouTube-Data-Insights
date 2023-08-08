import json
import awswrangler as wr
import pandas as pd



def lambda_handler(event, context):
    
    df_raw = wr.s3.read_json('s3://kd-youtubeanalysis-raw-useast1-dev/json-files/')
    required_json_data = pd.json_normalize(df_raw['items'])
    
    wr_response = wr.s3.to_parquet(
        df = required_json_data,
        path = 's3://kd-youtubeanalysis-cleansed-useast1-dev/cleansed_json_files/',
        database = 'kd-youtubeanalysis-cleansed-glue-database',
        table = 'cleansed_json_files',
        dataset = True,
        )
    
    return wr_response