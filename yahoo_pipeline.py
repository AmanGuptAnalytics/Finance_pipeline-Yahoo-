import dlt
import json
from yahoofinancials import YahooFinancials

@dlt.source
def yahoofin(date='2022-09-15'):
    return currency_exchange(date)

@dlt.resource(write_disposition="replace")
def currency_exchange(date):
    curr_pair=['USDINR=X']
    yahoo_financials_currency = YahooFinancials(curr_pair)
    daily_currency_prices = yahoo_financials_currency.get_historical_price_data(date, date, 'daily')
    yield daily_currency_prices


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='currency_exch', destination='bigquery', dataset_name='yahoo_fin2')

    load_info = pipeline.run(yahoofin())
    # pretty print the information on data that was loaded
    print(load_info)
