#
# Original script come from cryptodatadownload.com tutorial
# https://www.cryptodatadownload.com/blog/how-to-download-coinbase-price-data.html
#
#
# First import the libraries that we need to use
from datetime import datetime
from datetime import timedelta
import time
import pandas as pd
import requests
import json
def fetch_daily_data(symbol):
    pair_split = symbol.split('/')  # symbol must be in format XXX/XXX ie. BTC/EUR
    symbol = pair_split[0] + '-' + pair_split[1]
    # Supported granularities are 1,5,15 - minutes  1,6 - hours and 1 day
    #  60  300  900  3600  21600  86400 seconds respectively.
    granularity='60' # 1 minute granularity
    # start and end parameters are optional and could be either omitted or should be close enough 
    # to fit into 300 data points frame with a specific granularity
    # example: https://api.pro.coinbase.com/products/LTC-USD/candles?granularity=60
    #          https://api.pro.coinbase.com/products/LTC-USD/candles?granularity=60&start=2020-12-20T10:34:47&end=2020-12-20T13:15:00    
    # Time should be in ISO8601 format like that 2020-12-20T13:15:00.00Z.
    #start="2020-12-20T13:15"
    #end="2020-12-20T13:25"
    end=datetime.now()
    for i in range(100): # Read 100 data frames but you may read as deep as you want :)
       time.sleep(1)  #to avoid server side throttling
       print (i)
       start=end-timedelta(hours=5)
       url = f"https://api.pro.coinbase.com/products/{symbol}/candles?granularity={granularity}&start={start}&end={end}"
       end=start
       response = requests.get(url)
       if response.status_code == 200:  # check to make sure the response from server is good
          data = pd.DataFrame(json.loads(response.text), columns=['unix', 'low', 'high', 'open', 'close', 'volume'])
          data['date'] = pd.to_datetime(data['unix'], unit='s')  # convert to a readable date
          data['vol_fiat'] = data['volume'] * data['close']      # multiply the coin volume by closing price to approximate fiat volume

          # if we failed to get any data, print an error...otherwise write the file
          if data is None:
             print("Did not return any data from Coinbase for this symbol")
          else:
             data.to_csv(f'Coinbase_{pair_split[0] + pair_split[1]}_dailydata.csv',mode='a', index=False)
       else:
          print("Did not receieve OK response from Coinbase API")
if __name__ == "__main__":
   # we set which pair we want to retrieve data for
   pair = "LTC/USD"
   fetch_daily_data(symbol=pair)
