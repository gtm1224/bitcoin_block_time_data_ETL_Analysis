# See Ipython notebook:
# import pandas as pd
# from datetime import timedelta
# from bitcoin_block_data_collection_multi_thread import Get_Current_dir
# from matplotlib import pyplot as plt
# import numpy as np
#
#
# dir=Get_Current_dir()
# df=pd.read_csv(dir+'/data/data_preprocessing_copy.csv')
# df.sort_values(by=['height'], inplace=True)
# df.drop_duplicates(subset=['height'], inplace=True)
# df.reset_index(inplace=True, drop=True)
# df = df[['height','time']]
#
#
#
# # ====calculate time between two consecutive blocks
# df['time']=pd.to_datetime(df['time'])
# df['time_between_two_consecutive_blocks'] =(df['time'].shift(-1).copy()-df['time'].copy()).shift(1)
#
# # ====find two consecutive blocks mined more than 2 hours and mark the time
# #     note: we always mark the later block which is the block mined more than 2 hours from the former block
# condition = df['time_between_two_consecutive_blocks']>timedelta(hours=2)
# df.loc[condition,'time_marked']=df['time']
#
# # ====count the number of the incident(two consecutive blocks mined more than 2 hours) happens
# incident_happen_times = df['time_marked'].count()
#
# # ====get rid of other data since we only care about incidents
# incident_collection=df[df['time_marked'].notnull()][['time_marked']]
#
# # ====calculate the time interval between every two incidents
# incident_collection['time_between_each_incident']=(incident_collection['time_marked'].shift(-1)-incident_collection['time_marked']).shift(1)
# print(incident_collection)
#
# # ====calculate the average time that the incident(two consecutive blocks mined more than 2 hours) occurs
# average_time_incident_occurs = incident_collection['time_between_each_incident'].sum()/incident_collection['time_between_each_incident'].count()
#
# print('average_time_incident_occurs',average_time_incident_occurs)
#
#
# print(incident_collection)
#
#
#
#
# # ====convert time to unix timestamp
# df['timestamp']=((df['time'])-pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')
# print(df)
#
#
# plt.plot(df['height'],df['timestamp'])
# plt.show()