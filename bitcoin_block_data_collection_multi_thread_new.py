"""
author: Tim Guo
This is an updated version of multithread ETL.
get bitcoin block data by using blockchain data, bitcoin explorer API and python multi thread:
https://www.blockchain.com/api/blockchain_api
reason to choose a third party API see approach.txt
another way requires synchronizing bitcoin core on a machine and using bitcoin RPC API:
https://developer.bitcoin.org/reference/rpc/index.html
bitcoin block intro:
https://developer.bitcoin.org/devguide/block_chain.html
"""
from bitcoin_block_data_collection_multi_thread import Send_Get_Request_To_API
from bitcoin_block_data_collection_multi_thread import Get_Current_dir
import pandas as pd
import json
import concurrent.futures


dir = Get_Current_dir()
request_get_height_block_info = 'https://blockchain.info/block-height/%s?format=json'



def ETL(h):
    while True:
        try:
            print("start getting data>>:")
            request = request_get_height_block_info % (h)
            content = Send_Get_Request_To_API(url=request)
            content = json.loads(content)
            block_time =content['blocks'][0]['time']
            print('block_time:',block_time)
            df= pd.DataFrame({
                'height':h,
                 'time': block_time,
            },index=[h])
            print(df)
            df.to_csv(dir + '/data/data_preprocessing_new.csv', index=False, mode='a',header=None)
            print("getting successfully")
            break
        except Exception as e:
            print("error occur during getting data process: ",e)
            print("retrying>>:")



if __name__ == '__main__':
    pd.DataFrame(columns=['height', 'time']).to_csv(dir + '/data/data_preprocessing_new.csv', index=False)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(ETL,[h for h in range(650000,720001)])