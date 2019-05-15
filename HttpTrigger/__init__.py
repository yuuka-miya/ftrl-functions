import logging

import azure.functions as func

import pandas as pd


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    df = pd.read_csv("https://github.com/yuuka-miya/ftrl-data/raw/master/processed_data/201903/origin_destination_train_201903_wholemonth_20190423123747.csv")
        
    df = df[["DAY_TYPE", "ORIGIN_PT_CODE", "DESTINATION_PT_CODE", "TOTAL_TRIPS"]]
    
    df = df[df['TOTAL_TRIPS'] !=0]
    
    
    df = pd.concat([df[df["ORIGIN_PT_CODE"].str.contains("DT32")], df[df["DESTINATION_PT_CODE"].str.contains("DT32")]])
    df = df.drop_duplicates()

    df = df.sort_values(by='TOTAL_TRIPS', ascending=False)

    df = df[df["DAY_TYPE"] == "WEEKDAY"]

    return func.HttpResponse(df.to_json)
