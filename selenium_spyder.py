"""
author: Tim Guo
this script use selenium and chrome web driver to crawl
data from each page. It will mimic click event and go to next page
Note the selenium version is 3.141.0
"""
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import Options
import time
from random import randint
import pandas as pd
from bitcoin_block_data_collection_multi_thread import Get_Current_dir

# ====set up environment for Chrome web driver
options = Options()
options.add_experimental_option('excludeSwitches',['enable-automation'])
browser = webdriver.chrome.webdriver.WebDriver(executable_path="./chromedriver_97.exe", options=options)

# ====open the target web
browser.get("https://btc.com/btc/blocks")
browser.current_url

# ====set up the web database, choose all data
click_date=browser.find_element_by_xpath("//div[@class='ant-picker-input ant-picker-input-active']")
click_date.click()
choose_all =browser.find_element_by_xpath("(//li[@class='ant-picker-preset']/span)[1]")
choose_all.click()
time.sleep(10)
# <-----------------------------important click X to close cookie alert by hands---------------------------->

# ====set 100 lines of data per page:
bottom_page_drop_down=browser.find_element_by_xpath("//div[@class='Select_select__TBmtu Select_with-border__3-lix Select_size-xs__3yvxC Pagination_select-item__1q7Y0']/span/div")
bottom_page_drop_down.click()
choose_100_data_per_page=browser.find_element_by_xpath("(//div[@class='Select_dropdown__2L7JU']/div)[5]")
choose_100_data_per_page.click()
time.sleep(randint(1,3))




# ====get one row of data, here we only need height and time to make the program run faster
# ====if we want full data, 1 minute can extract 5 pages which is too slow,requires 24 hours to get full data!
def get_one_row_data(i):
    height = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[1]" % i).text
    height = int(str(height).replace(',',''))
    # miner = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[2]" % i).text
    t = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[3]" % i).text
    # tx_count = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[4]" % i).text
    # reward = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[5]" % i).text
    # size = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[6]" % i).text
    # fee = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[7]" % i).text
    # volume = browser.find_element_by_xpath("(//table/tbody/tr)[%s]/td[8]" % i).text
    # print({'height':height,'miner':miner,'time':t,'tx_count':tx_count,
    #         'reward':reward,'size':size,'fee':fee,'volume':volume})
    # return {'height':height,'miner':miner,'time':t,'tx_count':tx_count,
    #         'reward':reward,'size':size,'fee':fee,'volume':volume}
    return {'height':height,'time':t}


def load_df_to_csv(df,path=Get_Current_dir()+'/data/block_data_web_spyder.csv'):
    df.to_csv(path,mode='a',index=False,header=None)



def get_100_rows_of_data(num=100):
    df_list = []
    for i in range(1, num+1):
        while True:
            try:
                df_list.append(pd.DataFrame(get_one_row_data(i), index=[i]))
                break
            except Exception as e:
                print("error occur during getting data process: ", e)
                print("retrying>>:")
    return df_list



# ====construct data schema using pandas dataframe
# pd.DataFrame(columns=['height', 'miner', 'time', 'tx_count', 'reward', 'size', 'fee', 'volume']).to_csv(Get_Current_dir()+'/data/block_data_web_spyder.csv', index=False)
pd.DataFrame(columns=['height','time']).to_csv(Get_Current_dir()+'/data/block_data_web_spyder.csv', index=False)
# ====find latest block height
first = browser.find_element_by_xpath("(//table/tbody/tr)[1]/td[1]").text
first = int(str(first).replace(',',''))
# =====get remainder
remainder = first % 100

# ====go to the next page and extract data for each page
for i in range(7202):
    num=100
    if i==7200:
        num=remainder
    df=pd.concat(get_100_rows_of_data(num), ignore_index=True)
    print("data extracted=====>\n")
    print("page:",i)
    load_df_to_csv(df)
    next_page = browser.find_element_by_xpath("(//nav/li)[last()]/button")
    next_page.click()
    time.sleep(randint(1,2))

