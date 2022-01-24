"""
author: Tim Guo
get bitcoin block data by using blockchain data, bitcoin explorer API and python multi thread:
https://www.blockchain.com/api/blockchain_api
reason to choose a third party API see approach.txt
another way requires synchronizing bitcoin core on a machine and using bitcoin RPC API:
https://developer.bitcoin.org/reference/rpc/index.html
bitcoin block intro:
https://developer.bitcoin.org/devguide/block_chain.html
"""
from threading import Thread, current_thread
import json
import pandas as pd
import time
from datetime import datetime
from queue import Queue
import requests
from bitcoin_block_data_collection_single_thread import Get_Current_dir

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)


# =====construct API get request urls
# get recent block info: https://blockchain.info/latestblock
request_get_recent_block='https://blockchain.info/latestblock'
# request_get_height_block_info = https://blockchain.info/block-height/$block_height?format=json
request_get_height_block_info = 'https://blockchain.info/block-height/%s?format=json'




# ====define a function to send get request
def Send_Get_Request_To_API(url,max_try_num=10,sleep_time=5):
    """
    use python request lib to send get request to APIs
    :param url: APIs or urls
    :param max_try_num: maximum  times to try
    :param sleep_time: request failed, then sleep for a certain period
    :return: return response content
    """

    # some free proxies which do not work well
    # these free proxies are tested through test_proxy using concurrent pool
    # proxy={
    #     'https':'103.80.77.1:443',
    #     'http':'103.80.77.1:443',
    #     'https': '165.16.46.215:8080',
    #     'http': '165.16.46.215:8080',
    #     'https': '171.244.22.27:3128',
    #     'http': '171.244.22.27:3128',
    #
    #     'https': '103.80.83.166:8080',
    #     'http': '103.80.83.166:8080',
    #     'https': '182.253.28.124:8080',
    #     'http': '182.253.28.124:8080',
    #
    #     'https': '43.250.127.98:9001',
    #     'http': '43.250.127.98:9001',
    # }
    get_success = False  # check if get response successfully
    # start trying request:
    for i in range(max_try_num):
        try:
            headers= {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/81.0.4044.122 Safari/537.36'}
            content = requests.get(url=url,headers=headers,timeout=15).content
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
        # print('xxxxxxxxxxxxxxx URL PUSHED TO UNSUCCESSFUL URLS QUEUE xxxxxxxxxxxxxxx')
        # unsuccessful_urls.put(url)
        raise ValueError('reach error limit, please stop and check your code.')


# ====create a Queue to store extracted data
data_lake = Queue()

# # ====create a Queue to store unsuccessful urls
# unsuccessful_urls=Queue()

# ====define a function to extract data from API
def Get_Data(h):
    """
    use Send_Get_Request_To_API function to send get request to APIs
    use Transform_Data to parse Json content and store height and block
    time stamp data to a dataframe
    :param h: height
    :return: None
    """
    while True:
        try:
            print(current_thread(), "start getting data>>:")
            request = request_get_height_block_info % (h)
            content = Send_Get_Request_To_API(url=request)
            df=Transform_Data(content)
            data_lake.put(df)
            print("getting successfully")
            break
        except Exception as e:
            print("error occur during getting data process: ",e)
            print("retrying>>:")


# ====define a function to parse Json content to dataframe
def Transform_Data(content):
    """
    use Send_Get_Request_To_API function to send get request to APIs
    use Transform_Data to parse Json content and store height and block
    time stamp data to a dataframe
    :param h: height
    :return: None
    """
    content = json.loads(content)
    block_time = datetime.fromtimestamp(content['blocks'][0]['time'])
    h =content['blocks'][0]['height']
    df = pd.DataFrame(columns=['height', 'time'])
    df = df.append({'height': h, 'time': block_time}, ignore_index=True)
    return df


# ====define a function to append dataframe to a csv file
def Load_Data():
    print(current_thread(), "start saving data>>:")
    df =data_lake.get()
    path = Get_Current_dir() + '/data/block_time_multi_thread.csv'
    df.to_csv(path, index=False, mode='a',header=None)
    print('saving successfully')

# define a function to extract data from unsuccessful urls and load to CSV files
# def Retry_Unsuccessful_Urls():
#     url = unsuccessful_urls.get()
#     print('retry unsuccessful url: ',url)
#     content = Send_Get_Request_To_API(url=url)
#     content =json.loads(content)
#     block_time = datetime.fromtimestamp(content['blocks'][0]['time'])
#     h=content['height']
#     df = pd.DataFrame(columns=['height', 'time'])
#     df = df.append({'height': h, 'time': block_time}, ignore_index=True)
#     path = Get_Current_dir() + '/data/block_time_test.csv'
#     df.to_csv(path, index=False, mode='a', header=None)
#     print('saving successfully')


if __name__ == '__main__':
    # ==== get recent height of bitcoin block
    content = Send_Get_Request_To_API(url=request_get_recent_block)
    content = json.loads(content)
    recent_height = content['height']
    print('recent height of bitcoin block: ', recent_height)
    path=Get_Current_dir() + '/data/block_time_multi_thread.csv'
    # ====start fetching data
    pd.DataFrame(columns=['height','time']).to_csv(path, index=False)
    counter =0
    leap= 50
    remainder=recent_height % leap
    while counter < recent_height:
        for height in range(counter,counter+leap):
            t1 = Thread(target=Get_Data,args=(height,))
            t2 = Thread(target=Load_Data)
            t1.start()
            t2.start()
        # while not unsuccessful_urls.empty():
        #     t3 = Thread(target=Retry_Unsuccessful_Urls)
        #     t3.start()
        time.sleep(1)
        counter+=leap
        if counter == recent_height - remainder:
            leap=remainder+1

