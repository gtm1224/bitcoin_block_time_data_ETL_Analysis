"""
author: Tim Guo
this script will use multi thread to get missing data and append to
our raw data file.
"""
from find_missing import find_missing_data
from bitcoin_block_data_collection_multi_thread import Send_Get_Request_To_API
from bitcoin_block_data_collection_multi_thread import Get_Current_dir
import pandas as pd
import json
import concurrent.futures
from datetime import datetime

dir = Get_Current_dir()
df = pd.read_csv(dir + '/data/data_preprocessing_new_combine.csv')
missing_h=find_missing_data(df)
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
            df.to_csv(dir + '/data/data_preprocessing_new_combine.csv', index=False, mode='a',header=None)
            print("getting successfully")
            break
        except Exception as e:
            print("error occur during getting data process: ",e)
            print("retrying>>:")



if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(ETL,missing_h)

