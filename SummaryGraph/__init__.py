import logging

import azure.functions as func

import urllib, json

import pandas as pd
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot
from io import BytesIO
import base64

#i thought azure would do this for me but no
http_response_headers = {
"Access-Control-Allow-Origin": "*",
"Content-Type": "text/html"
}

def plot(self):
        image = BytesIO()
        x = numpy.linspace(0, 10)
        y = numpy.sin(x)
        pyplot.plot(x, y)
        pyplot.savefig(image, format='png')
        return base64.encodestring(image.getvalue())

def main(req: func.HttpRequest) -> func.HttpResponse:

    code = req.params.get('ptcode')
    type = req.params.get('type')
    limit = req.params.get('limit')
    direction = req.params.get('direction')
    
    period = req.params.get('period')
    
    url = "https://yuuka-miya.github.io/ftrl-data/data_list.json"

    with urllib.request.urlopen(url) as f:
        data = json.loads(f.read())

    interchange_codes = data["interchange_codes"]
    
    if not code:
        return func.HttpResponse("Please pass a ptcode on the query string",status_code=400)
    if not type:
        return func.HttpResponse("Please pass a type on the query string",status_code=400)
    if not direction:
        return func.HttpResponse("Please pass a direction on the query string",status_code=400)
       
    if code in interchange_codes:
        code = interchange_codes[code]
        
    url = "https://github.com/yuuka-miya/ftrl-data/raw/master/processed_data/summary.csv"
    title = "passengers on "
    
    df_in = pd.read_csv(url, header=[0, 1])
    df_in = df_in.drop([0])
    temp = df_in.columns.to_numpy()
    temp[0] = ('DAY_TYPE', None)
    temp[1] = ('PT_CODE', None)
    df_in.columns = pd.MultiIndex.from_tuples(temp)
    df_in.columns.set_names("month", level = 1)
    df_in = df_in.set_index([ ('DAY_TYPE', None), ('PT_CODE', None)])
    df_in.index.set_names("DAY_TYPE" , level=0)
    df_in.index.set_names("PT_CODE" , level=1)
    
    if type is "1":
        df = df_in.loc['WEEKDAY'].loc[code]
        title = title + "WEEKDAY"
    if direction is "2":
        df = df_in.loc['WEEKENDS/HOLIDAY'].loc[code]
        title = title + "WEEKENDS/HOLIDAY"
    image = BytesIO()
    if direction is "1":
        title = "Entering " + title + " at " + code
        inplot = df['TOTAL_TAP_IN_VOLUME'].plot(kind="bar", title = title).get_figure()
    if direction is "2":
        title = "Exiting " + title + " at " + code
        inplot = df['TOTAL_TAP_OUT_VOLUME'].plot(kind="bar", title = title).get_figure()
        
    inplot.savefig(image, format='png')
    d = base64.encodestring(image.getvalue())
    
    if "raw" in req.params.keys():
        return func.HttpResponse(d.decode('utf8'), headers = {"Access-Control-Allow-Origin": "*"})

    
    return func.HttpResponse('''<img src="data:image/png;base64,%s"/> ''' %(d.decode('utf8')), headers = http_response_headers)
