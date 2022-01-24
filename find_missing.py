"""
author: Tim Guo
this script defined a function that can store missing block heights to a list
the way is by concatenating a dataframe with all the heights and empty time
to the preprocessing data frame. Then we drop the duplicate rows by height,
the row with empty time will have the height that is missing.
"""
import pandas as pd
from datetime import timedelta
from bitcoin_block_data_collection_multi_thread import Get_Current_dir

# define a function to find missing data
def find_missing_data(df):
    last_block_height=df.iloc[-1,0]
    # construct a df to check missing heights
    df_check_missing_values= pd.DataFrame({
        'height': range(last_block_height+1),
        'time': ['' for i in  range(last_block_height+1)],
    })
    df_final = pd.concat([df, df_check_missing_values], ignore_index=True)
    df_final.drop_duplicates(subset=['height'], inplace=True, keep='first')
    df_final.sort_values(by=['height'], inplace=True)
    df_final.reset_index(inplace=True, drop=True)
    df_request = df_final[df_final['time'] == '']['height']
    missing_height = list(df_request)
    print('df_request\n', missing_height)
    print('number of missing heights: ',len(missing_height))
    return missing_height

if __name__ == '__main__':
    dir = Get_Current_dir()
    df = pd.read_csv(dir + '/data/data_preprocessing.csv')
    find_missing_data(df)








