import dlt
import json
from yahoofinancials import YahooFinancials

@dlt.source
def yahoofin(start_date='2022-02-01',end_date='2022-03-01'):
    
    return currency_exchange(start_date, end_date )

@dlt.resource(write_disposition="append")
def currency_exchange(start_date,end_date ):
    curr_pair=['USDINR=X']
    print('This is the end date', end_date)
    dt = dlt.state().setdefault("date", [])
    print('what we got in state or default ',dt)
    dlt.state()["date"] = '2021-2-12'
    dt= dlt.state().setdefault("date", [])
    print('what we set to state',dt)

    yahoo_financials_currency = YahooFinancials(curr_pair)
    daily_currency_prices = yahoo_financials_currency.get_historical_price_data(start_date, end_date, 'daily')
    yield daily_currency_prices
    

if __name__=='__main__':
    # configure the pipeline with your destination details

    pipeline = dlt.pipeline(pipeline_name='currency_exch', destination='bigquery', dataset_name='yahoo_fin3', full_refresh='True')
    
    load_info = pipeline.run(yahoofin())
    # pretty print the information on data that was loaded
    print(load_info)
    
