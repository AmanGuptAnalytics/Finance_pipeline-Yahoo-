import dlt
import requests
import sys
import pandas as pd
import json
                
@dlt.source
def weather_source(api_key=dlt.secrets.value):
    return get_weather_data(location ='London' , start_date ='2021-01-01', end_date ='2021-02-01', api_key=api_key)


@dlt.resource(write_disposition="append")
def get_weather_data(location ,start_date,end_date,api_key):
       
        print(location,start_date,end_date,api_key)
        since_date = dlt.state().setdefault("incremental_dict", {'last_loaded_date':'1970-01-01'})
        url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}'
        params = {
        'unitGroup': 'metric',
        'include': 'days',
        'key': api_key,
        'contentType': 'json' }

        #requesting the data 
        
        if since_date['last_loaded_date'] < end_date and since_date['last_loaded_date'] > start_date:
            start_date = since_date['last_loaded_date']
            print("Start date we are using is", start_date)
            print(location,start_date,end_date,api_key)
            response = requests.get(url, params)
            since_date['last_loaded_date'] = end_date
            yield response.json()
            
        else:
            print("Start date we are using is", start_date)
            print(location,start_date,end_date,api_key)
            response = requests.get(url, params)
            since_date['last_loaded_date'] = end_date
            yield response.json()
            


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='weather12', destination='bigquery', dataset_name='weather11', full_refresh=False)

    load_info = pipeline.run(weather_source())
    # pretty print the information on data that was loaded
    print(load_info)
