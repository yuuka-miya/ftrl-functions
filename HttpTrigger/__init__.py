import logging

import azure.functions as func

import pandas as pd

#i thought azure would do this for me but no
http_response_headers = {
"Access-Control-Allow-Origin": "*"
}

interchange_codes = {
    "BP1": "NS4",
    "CC15": "NS17",
    "CE2": "NS27",
    "CC9": "EW8",
    "DT14": "EW12",
    "NE3": "EW16",
    "CC22": "EW21",
    "DT35": "CG1",
    "CC29": "NE1",
    "DT19": "NE4",
    "DT12": "NE7",
    "CC13": "NE12",
    "STC": "NE16",
    "PTC": "NE17",
    "DT26": "CC10",
    "DT9": "CC19",
    "DT16": "CE1",
    "TE2": "NS9",
    "TE9": "CC17",
    "TE11": "DT10",
    "TE14": "NS22",
    "TE17": "EW16",
    "TE20": "NS27",
    "TE31": "DT37",
    "FL1": "CC32",
    "JS1": "NS4",
    "JS8": "EW27",
    "JE5": "EW24-NS1"
    
    }

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    code = req.params.get('ptcode')
    type = req.params.get('type')
    limit = req.params.get('limit')
    direction = req.params.get('direction')
    logging.info(type)
    
    if not code:
        return func.HttpResponse("Please pass a ptcode on the query string",status_code=400)
       
    if code in interchange_codes:
        code = interchange_codes[code]
    
    df = pd.read_csv("https://github.com/yuuka-miya/ftrl-data/raw/master/processed_data/201903/origin_destination_train_201903_wholemonth_20190423123747.csv")
        
    
    df = df[["DAY_TYPE", "ORIGIN_PT_CODE", "DESTINATION_PT_CODE", "TOTAL_TRIPS"]]
    
    df = df[df['TOTAL_TRIPS'] !=0]

    if type is "2":
      df = df[df["DAY_TYPE"] == "WEEKENDS/HOLIDAY"]
    else:
      df = df[df["DAY_TYPE"] == "WEEKDAY"]
    
    if direction is "1":
      df = pd.concat([df[df["ORIGIN_PT_CODE"].str.contains(code)]])
    if direction is "2":
      df = pd.concat([df[df["DESTINATION_PT_CODE"].str.contains(code)]])
    else:
      df = pd.concat([df[df["ORIGIN_PT_CODE"].str.contains(code)], df[df["DESTINATION_PT_CODE"].str.contains(code)]])    
    
    df = df.drop_duplicates()

    df = df.sort_values(by='TOTAL_TRIPS', ascending=False)
      
    if limit:
      try:
        lim = int(limit)
        df = df.head(lim)
      except ValueError:
        pass

    return func.HttpResponse(df.to_json(orient='records'), headers = http_response_headers)
