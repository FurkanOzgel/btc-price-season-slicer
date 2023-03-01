import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
from os import system

def convert(date_string):
    dt_obj = dt.datetime.strptime(date_string,
                           '%Y-%m-%d %H:%M:%S')
    millisec = dt_obj.timestamp() * 1000
    return int(millisec)

def get_bars(symbol, interval , start, end):
   root_url = 'https://api.binance.com/api/v1/klines'
   url = root_url + '?symbol=' + symbol + '&interval=' + interval + "&startTime=" + str(convert(start))+ "&endTime="+str(convert(end))
   data = json.loads(requests.get(url).text)
   df = pd.DataFrame(data)
   df.columns = ['open_time',
                 'o', 'h', 'l', 'c', 'v',
                 'close_time', 'qav', 'num_trades',
                 'taker_base_vol', 'taker_quote_vol', 'ignore']

   df.insert(1,"Open_Time",[dt.datetime.fromtimestamp(x/1000.0) for x in df.open_time],True)
   df.insert(2,"Close_Time",[dt.datetime.fromtimestamp(x/1000.0) for x in df.close_time],True)
   df.insert(4,"C",[x for x in df.c],True)
   df.insert(3,"O",[x for x in df.o],True)
   


   del df["c"]
   del df["o"]
   del df["close_time"]
   del df["open_time"]
   del df["ignore"]
   return df

past = True
first = True

while past:
    coinData = []

    with open("Price_Data_Daily/last_time.txt", "r", encoding="utf-8") as file:
        coinData.append(file.read().split(" ",1))
    
    for coin, coinTime in coinData:
        hours = 1800
        hours_added = timedelta(hours = hours)
        date_time_obj = datetime.strptime(coinTime, '%Y-%m-%d %H:%M:%S')
        mainDf = pd.DataFrame({"Open_Time":[],"Close_Time":[],
                               "O":[],"C":[],"h":[],"l":[],"v":[],
                               "qav":[],"num_trades":[],
                               "taker_base_vol":[],"taker_quote_vol":[]
                               })

        while True:
            try:
                future_date_and_time = date_time_obj + hours_added
                df2 = get_bars(coin,"1d",str(date_time_obj), str(future_date_and_time))
                new = df2.iloc[-2]
                date_time_obj =  future_date_and_time
                mainDf = pd.concat([mainDf, df2], ignore_index=True)
                print(df2)
                first = False
                
            except:
                if date_time_obj > datetime.now():
                    if first:
                        print("We came now.")
                        past = False
                        break
                    else:
                        oldDf = pd.read_csv("Price_Data_Daily/"+coin+"_Data.csv", index_col=0)
                        mainDf = pd.concat([oldDf, mainDf[:-1]], ignore_index=True)
                        mainDf.to_csv("Price_Data_Daily/"+coin+"_Data.csv")
                        with open("Price_Data_Daily/last_time.txt", "w", encoding="utf-8") as file:
                            file.write(coin +" "+ str(new.Close_Time)[:19])
                        past = False
                        print("We came now.")
                        break
                else:
                    print("We came now.")
                    past = False
                    break
