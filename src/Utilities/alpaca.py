import os
from dotenv import load_dotenv
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame
import multiprocessing as mp

class Alpaca:

    key_id: str
    secret_key: str

    def __init__(self) -> None:
        
        load_dotenv('.env')
        self.key_id = os.getenv('ALPACA_KEY_ID')
        self.secret_key = os.getenv('ALPACA_SECRET_KEY')

    def get_ticket_list(self):
        
        api = REST()
        active_assets = api.list_assets(status='active')  # you could leave out the status to also get the inactive ones
        asset_list = []

        for asset in active_assets:
            asset_list.append(asset.symbol)
        
        return asset_list

def download_historical(sym: str):

        api = REST()
        sym_df = api.get_bars(sym, TimeFrame.Minute, "2021-04-20", "2022-04-20", adjustment='raw').df
        sym_df.to_csv(f'src/data/{sym}.csv')

def main():

    alpaca = Alpaca()
    
    tickers = alpaca.get_ticket_list()
    pool = mp.Pool(os.cpu_count())

    for ticker in tickers:
        pool.apply_async(download_historical, args=(ticker,))
    
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()