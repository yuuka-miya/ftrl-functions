import logging

import azure.functions as func

import pandas as pd

import urllib, json

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
    
    code = req.params.get('ptcode')
    type = req.params.get('type')
    limit = req.params.get('limit')
    direction = req.params.get('direction')
    
    period = req.params.get('period')
    logging.info(to_sum)
    
    url = "https://yuuka-miya.github.io/ftrl-data/data_list.json"

    with urllib.request.urlopen(url) as f:
        data = json.loads(f.read())

    interchange_codes = data["interchange_codes"]
    if period not in data["data_packs"]["train"]:
        return func.HttpResponse("Period not found!",status_code=400)
    
    if not code:
        return func.HttpResponse("Please pass a ptcode on the query string",status_code=400)
       
    if code in interchange_codes:
        code = interchange_codes[code]
        
    url = "https://github.com/yuuka-miya/ftrl-data/raw/master/processed_data/" + period + "/" + data["data_packs"]["train"][period]
    
    df = pd.read_csv(url)
    df = df[["DAY_TYPE", "ORIGIN_PT_CODE", "DESTINATION_PT_CODE", "TOTAL_TRIPS"]]
    
    df = df[df['TOTAL_TRIPS'] !=0]
    
    if type is "2":
      df = df[df["DAY_TYPE"] == "WEEKENDS/HOLIDAY"]
    else:
      df = df[df["DAY_TYPE"] == "WEEKDAY"]
    
    
    if direction is "1":
      df = df.groupby(['ORIGIN_PT_CODE']).sum()

    else:
      df = df.groupby(['DESTINATION_PT_CODE']).sum()
    return func.HttpResponse(df.to_json(orient='columns'), headers = http_response_headers)
