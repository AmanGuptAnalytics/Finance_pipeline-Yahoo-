import dlt
import json
from yahoofinancials import YahooFinancials

@dlt.source
def yahoofin(start_date='2022-01-01',end_date='2022-03-01'):
    return currency_exchange(start_date, end_date )

@dlt.resource(write_disposition="replace")
def currency_exchange(start_date,end_date ):
    curr_pair=['USDINR=X']
    print('This is the end date', end_date)
    since_date = dlt.state().setdefault("date", 0)
    print('The date is as follows', since_date) # will print what exists, which initially is nothing, so defaults to 0. On second run it prints 12345
    since_date = 12345
    yahoo_financials_currency = YahooFinancials(curr_pair)
    daily_currency_prices = yahoo_financials_currency.get_historical_price_data(start_date, end_date, 'daily')
    yield daily_currency_prices


if __name__=='__main__':
    # configure the pipeline with your destination details

    pipeline = dlt.pipeline(pipeline_name='currency_exch', destination='bigquery', dataset_name='yahoo_fin3')
    load_info = pipeline.run(yahoofin())
    # pretty print the information on data that was loaded
    print(load_info)
