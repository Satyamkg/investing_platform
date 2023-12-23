import requests
import json
import os
import pandas  as pd 
from datetime import datetime
import time
start_date_range = '2023-11-01'
end_date_range = '2023-11-20'
final_date = '2023-11-03'
dict_rates = {}

def extract_raw_data(date_ref):
    #api_key = '24374ab81efea73ca8d0a3e17bbbab26'
    api_key = os.environ["api_key"]
    url = f"http://data.fixer.io/api/{date_ref}"
    params= {"access_key": api_key}
    response = requests.get(url, params= params)
    raw_data = response.text
    final_data = json.loads(raw_data)
    print(date_ref)
    print("final_data")
    print(final_data)
    rates = final_data['rates']
    print("rates")
    print(rates)
    dict_rates[date_ref] = rates
    df = pd.DataFrame(dict_rates)
    return df 

def df_expand_dates(start, end ,ref_df, ref_list, run_current_date = False):
    final_df = pd.DataFrame()
    dates_n = pd.DataFrame()
    dates_n = pd.date_range(start=start, end = end , freq='D')
    dates_list = [str(x.date()) for x in dates_n]
    print(f"printing dates list is {dates_list}")
    print(f"printing ref list is {ref_list}")
    result_1 = [item for item in dates_list if item not in ref_list]
    print(f"prining result_1 is {result_1}")
    for n in result_1:
        temp = extract_raw_data(date_ref= n)
        final_df = final_df._append(temp)
        time.sleep(0.5)
    if run_current_date == True:
        current_date_str =  datetime.today().date().strftime("%Y-%m-%d" )
        if current_date_str not in dates_list:
            temp = extract_raw_data(current_date_str)
            final_df = final_df._append(temp)
            dates_list = dates_list.append(current_date_str)
            print(final_df)
    print(final_df)
    print(ref_df)
    result = pd.concat([final_df, ref_df])
    return result 

def write_to_csv(final_df, filename):
    final_df.to_csv(filename)

def current_date_list():
    exist_df = pd.read_csv('investing_platform.csv', index_col=0)
    exist_dates_list = list(exist_df.columns)
    print(exist_dates_list)
    return exist_df, exist_dates_list

# also check what dates are already present in the current dataframe and remove the data if it is already there
#result = df_expand_dates(run_current_date = True)
#write_to_csv(result)
def execute_func():

    exist_df, exist_dates = current_date_list()
    output = df_expand_dates( '2023-10-20' , '2023-11-20', exist_df,exist_dates, run_current_date = False)
    write_to_csv(output,'investing_platform.csv')
    print(output.columns)
    print(output.T)
    df_output = output.T 
    write_to_csv(df_output, 'ingest_csv.csv')

if __name__ == "__main__":
    execute_func()
