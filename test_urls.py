"""
author: Tim Guo
this is a test file to test our API links
API links also get tested using Postman
"""
import json
import pandas as pd
import time
from datetime import timedelta,datetime
import requests

dt = datetime.now().date()
print(dt)
dt = datetime(2018,1,1)
milliseconds = int(round(dt.timestamp() * 1000))
print(milliseconds)
start_date = pd.to_datetime('2018-01-01 00:00:00')

end_date = start_date + timedelta(days=38)
date_list=[]
date = start_date
# while date<=pd.to_datetime(end_date):
#     date_list.append(str(date))
#     date+=datetime.timedelta(days=1)


epoch = datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

print(unix_time_millis(start_date))
exit()
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)

# =====construct API get request
# get recent block info: https://blockchain.info/latestblock
# for block height: https://blockchain.info/block-height/$block_height?format=json


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
    proxy={
        'https':'103.80.77.1:443',
        'http':'103.80.77.1:443',
        'https': '165.16.46.215:8080',
        'http': '165.16.46.215:8080',
        'https': '171.244.22.27:3128',
        'http': '171.244.22.27:3128',
    }

    for i in range(max_try_num):
        try:
            content = requests.get(url=url,timeout=25).content
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

content =Send_Get_Request_To_API('https://chain.api.btc.com/v3/block/date/20090109')
print(content)
content =json.loads(content)
print(content['data'])
# for listt in content['data']:
#     print(listt['height'])
#     print(listt['timestamp'])
# print(content['data'][0])