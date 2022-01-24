"""
author: Tim Guo
get bitcoin block data by using blockchain data API:
https://www.blockchain.com/api/blockchain_api
reason to choose a third party API see approach.txt
another way requires synchronize bitcoin core on a machine and use bitcoin RPC API:
https://developer.bitcoin.org/reference/rpc/index.html
bitcoin block intro:
https://developer.bitcoin.org/devguide/block_chain.html
"""
from urllib.request import urlopen
import requests
import json
from random import randint
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)


# =====construct API get request
# get recent block info: https://blockchain.info/latestblock
# for block height: https://blockchain.info/block-height/$block_height?format=json
request_get_recent_block='https://blockchain.info/latestblock'
request_get_height_block_info = 'https://blockchain.info/block-height/%s?format=json'

# ====define a function to get current directory
def Get_Current_dir():
    """
    use python os to get current dir
    :param: None
    :return: return a string of current dir
    """
    cwd = os.getcwd()
    data_dir = cwd.replace("\\", "/")
    return data_dir



# ====define a function to send get request
def Send_Get_Request_To_API(url,max_try_num=10,sleep_time=5):
    """
    use python urlopen function to send get request to APIs
    :param url: APIs or urls
    :param max_try_num: maximum  times to try
    :param sleep_time: request failed, then sleep for a certain period
    :return: return response content
    """
    get_success = False# check if get response successfully
    # start trying request
    for i in range(max_try_num):
        try:
            content = urlopen(url=url,timeout=10).read().decode()
            get_success = True
            break
        except Exception as e:
            print('something went wrong，times：', i+1, 'error message：', e)
            print('will retry after the sleep')
            time.sleep(sleep_time)
        # check if receives the response
    if get_success:
        return content
    else:
        raise ValueError('reach error limit, please stop and check your code.')




if __name__ == '__main__':
    # ==== get recent height of bitcoin block
    content = Send_Get_Request_To_API(url=request_get_recent_block)
    content = json.loads(content)
    recent_height = content['height']
    print('recent height of bitcoin block: ', recent_height)

    # ====start fetching data and write data to csv file
    for height in range(182301,recent_height+1):
        print('start fetching data======================================>')
        request = request_get_height_block_info % (height)
        content = Send_Get_Request_To_API(url=request)
        content = json.loads(content)
        block_time = datetime.fromtimestamp(content['blocks'][0]['time'])
        df = pd.DataFrame(columns=['height', 'time'])
        df = df.append({'height': height, 'time': block_time}, ignore_index=True)
        print(df)
        print('fetching successfully')
        print('======================================>start saving data')
        path = Get_Current_dir() + '/data/block_time_single_thread_test.csv'
        if os.path.exists(path):
            df.to_csv(path, header=None, index=False, mode='a')
        else:
            pd.DataFrame(columns=['data collected by Tim Guo']).to_csv(path, index=False)
            df.to_csv(path, index=False, mode='a')
        print('saving successfully')

# print(recent_height)
# print(type(recent_height))


