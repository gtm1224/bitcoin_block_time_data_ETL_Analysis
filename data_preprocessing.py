"""
author: Tim Guo
this script concatenate all the raw data. It convert all time to local which is 'US/Pacific'
it applies sort, drop duplicates and store the concatenated data frame to a csv file.
"""

import pandas as pd
from bitcoin_block_data_collection_multi_thread import Get_Current_dir


# ====read data and convert to local time
dir=Get_Current_dir()
df1=pd.read_csv(dir+'/data/block_data_web_spyder.csv')
df1=df1[['height','time']]
df1.sort_values(by=['height'], inplace=True)
df1.drop_duplicates(subset=['height'], inplace=True)
df1.reset_index(inplace=True, drop=True)


df2=pd.read_csv(dir+'/data/block_time_single_thread_test.csv')
df2=df2[['height','time']]
df2.sort_values(by=['height'], inplace=True)
df2.drop_duplicates(subset=['height'], inplace=True)
df2.reset_index(inplace=True, drop=True)

# ====note since multi thread running on a Singapore Cloud Server, we need to convert to local time
df3=pd.read_csv(dir+'/data/block_time_multi_thread.csv')
df3=df3[['height','time']]
df3['time']=pd.to_datetime(df3['time']).dt.tz_localize('Asia/Singapore').dt.tz_convert('US/Pacific')
df3['time']=df3['time'].transform(lambda x:str(x)[:19])
df3.sort_values(by=['height'], inplace=True)
df3.drop_duplicates(subset=['height'], inplace=True)
df3.reset_index(inplace=True, drop=True)

# ====combine all the dataframes
df= pd.concat([df3,df2,df1],ignore_index=True)
df.sort_values(by=['height'], inplace=True)
df.drop_duplicates(subset=['height'], inplace=True)
df.reset_index(inplace=True, drop=True)
print(df)
df.to_csv(dir+'/data/data_preprocessing.csv')


