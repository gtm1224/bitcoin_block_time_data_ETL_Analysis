"""
author: Tim Guo
this script use multithread to test working proxies.
"""
import requests
import random
import pandas as pd
import concurrent.futures
from bitcoin_block_data_collection_multi_thread import Get_Current_dir
from random import randint
import json
#opens a csv file of proxies and prints out the ones that work with the url in the extract function

proxylist = []

df=pd.read_csv(Get_Current_dir()+'/data/Free_Proxy_List.csv')
for i in range(len(df)):
    proxy=str(df.iloc[i]['ip'])+':'+str(df.iloc[i]['port'])
    proxylist.append(proxy)

def extract(proxy):
    #this was for when we took a list into the function, without conc futures.
    #proxy = random.choice(proxylist)
    headers = headers= {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/81.0.4044.122 Safari/537.36'}
    try:
        block_height = randint(0,50000)
        test_url ='https://bitcoinexplorer.org/api/block/%s' % (block_height)
        content = requests.get(test_url, headers=headers, proxies={'http' : proxy,'https': proxy}, timeout=5).content
        content= json.loads(content)
        print(content['time'], ' | Works')
        print(proxy)
    except:
        pass
    return proxy

with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(extract, proxylist)