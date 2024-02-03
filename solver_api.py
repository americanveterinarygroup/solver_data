import os
import math
import json
import logging
import requests
import pandas as pd
from io import StringIO

solver_token = os.getenv('SOLVER_API_TOKEN')

def get_object(url):
    payload = {}
    headers = {'Content-Type': 'application/json',
                'Authorization': f'Bearer {solver_token}'
            }

    response = requests.request("GET", url, headers=headers, data=payload)
    sc = response.status_code

    if sc != 200: 
        logging.error(f'unexpected status code: {sc} | {url}')
    else: 
        logging.info(f'status code: {sc} | {url}')

    return response.json()



def get_object_list():
    url = 'https://us.app.solverglobal.com/api/v1/'
    return get_object(url)



def get_paginated_data(url, dt):
    json_data = []
    ### iterate over paging by limit =+ 10,000
    try:
        low = 10000
        high = 500000
        for i in range(math.ceil(high / low)):
            j = str(i * low)
            increment_page = '&$skip=' + j + '&$top=' + j
            page_url = url + increment_page        

            json_response = get_object(page_url)
            # set json to str literal for subsequent pd.read_json() | not setting to str literal will be deprecated in future
            d = StringIO(json.dumps(json_response['data']))

            json_data.append(pd.read_json(d, orient='list'))
            logging.info(f'page [{j}] - normalized json appended to list')

            if json_response['nextUrl'] is None: 
                logging.info(f'break: all data received for filtered period {dt}')
                break
        
        df = pd.concat(json_data, sort=False)
        df = df.astype(str)

        return df
    except Exception as ex:
        logging.error(f'{type(ex).__name__}: {ex}')