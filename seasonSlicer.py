import pandas as pd

while True:
    
    mainData = pd.read_csv("Price_Data/BTCUSDT_Data.csv", index_col=[0])      ### For all data
    bullSaver = False
    bearSaver = False
    extrabool = False
    saver = False
    bullLastBreaker = False
    notDelete = False
    bearLastBreaker = False
    forBreaker = False
    minPrice = [100000000]
    maxPrice = [0]
    ramSeason = pd.DataFrame(columns=["Open_Time","Close_Time",
                           "Price_Direction",
                           "O","C","h","l","v",
                           "qav","num_trades",
                           "taker_base_vol","taker_quote_vol"])
    season = pd.DataFrame(columns=["Open_Time","Close_Time",
                           "Price_Direction",
                           "O","C","h","l","v",
                           "qav","num_trades",
                           "taker_base_vol","taker_quote_vol"])
    deletedDf = pd.DataFrame(columns=["Open_Time","Close_Time",
                           "Price_Direction",
                           "O","C","h","l","v",
                           "qav","num_trades",
                           "taker_base_vol","taker_quote_vol"])


    for row in mainData.iterrows():

        if float(row[1]["O"]) < float(minPrice[0]):                   # Min ve max noktasını almak için
            minPrice = [float(row[1]["O"]), row[1]["Open_Time"]]
        if float(row[1]["O"]) > float(maxPrice[0]):
            maxPrice = [float(row[1]["O"]), row[1]["Open_Time"]]

        distance = maxPrice[0] - minPrice[0]

        if distance > maxPrice[0]*0.19:

            if maxPrice[1] > minPrice[1]:   #Boğa sezonu

                for data in mainData.iterrows():       # Geçici boğa sezonu tanımlanıyor.
                    if data[1]["Open_Time"] == minPrice[1]:
                        bullSaver = True
                    if bullSaver:
                        newDf = pd.DataFrame({"Open_Time":[data[1]["Open_Time"]],"Close_Time":[data[1]["Close_Time"]],
                                               "Price_Direction":[data[1]["Price_Direction"]],
                                               "O":[data[1]["O"]],
                                               "C":[data[1]["C"]],"h":[data[1]["h"]],"l":[data[1]["l"]]
                                               ,"v":[data[1]["v"]],
                                               "qav":[data[1]["qav"]],"num_trades":[data[1]["num_trades"]],
                                               "taker_base_vol":[data[1]["taker_base_vol"]],"taker_quote_vol":[data[1]["taker_quote_vol"]]})
                        ramSeason = pd.concat((ramSeason, newDf), axis=0, join='outer')                  
                    if data[1]["Open_Time"] == maxPrice[1]:
                        bullSaver = False
                        bullFinishIndex = data[0]
                minPrice = [100000000]
                for extraRow in mainData.iterrows():
                    if extraRow[0] == bullFinishIndex:
                        extrabool = True
                    if extrabool:
                        if float(extraRow[1]["O"]) < float(minPrice[0]):                   
                            minPrice = [float(extraRow[1]["O"]), extraRow[1]["Open_Time"]]
                        if float(extraRow[1]["O"]) > float(maxPrice[0]):
                            maxPrice = [float(extraRow[1]["O"]), extraRow[1]["Open_Time"]]

                        distance = maxPrice[0] - minPrice[0]

                        if distance > maxPrice[0]*0.19:
                            if maxPrice[1] > minPrice[1]:
                                minPrice = [100000000]
                            else:
                                bullLastBreaker = True
                                print("BOĞA")
                                for i in mainData.iterrows():       # Kalıcı boğa sezonu tanımlanıyor.
                                    if i[1]["Open_Time"] == ramSeason.iloc[0]["Open_Time"]:
                                        saver = True
                                    if saver:
                                        newDf = pd.DataFrame({"Open_Time":[i[1]["Open_Time"]],"Close_Time":[i[1]["Close_Time"]],
                                                               "Price_Direction":[i[1]["Price_Direction"]],
                                                               "O":[i[1]["O"]],
                                                               "C":[i[1]["C"]],"h":[i[1]["h"]],"l":[i[1]["l"]]
                                                               ,"v":[i[1]["v"]],
                                                               "qav":[i[1]["qav"]],"num_trades":[i[1]["num_trades"]],
                                                               "taker_base_vol":[i[1]["taker_base_vol"]],"taker_quote_vol":[i[1]["taker_quote_vol"]]})
                                        season = pd.concat((season, newDf), axis=0, join='outer', ignore_index=True)                  
                                    if i[1]["Open_Time"] == maxPrice[1]:
                                        saver = False
                                        with open("seasonCount.txt","r") as file:
                                            for i in file.readlines():
                                                seasonCount = i
                                        season.to_csv(("Seasons/"+"BullSeason---"+i+".csv").replace(":","."))

                                        with open("seasonCount.txt","w") as file:
                                            file.write(str(int(i)+1))
                                        
                                        for i in mainData.iterrows():
                                            if i[1]["Open_Time"] == maxPrice[1]:
                                                deletedDf = mainData.iloc[i[0]:len(mainData)]                   
                                                deletedDf.reset_index(drop=True, inplace=True)
                                                deletedDf.to_csv("Price_Data/BTCUSDT_Data.csv")
                                                forBreaker = True
                                                break

                                                
    
                            if bullLastBreaker:
                                break                    
            else:                           #Ayı sezonu
                for data in mainData.iterrows():       # Geçici ayı sezonu tanımlanıyor.
                    if data[1]["Open_Time"] == maxPrice[1]:
                        bearSaver = True
                    if bearSaver:
                        newDf = pd.DataFrame({"Open_Time":[data[1]["Open_Time"]],"Close_Time":[data[1]["Close_Time"]],
                                               "Price_Direction":[data[1]["Price_Direction"]],
                                               "O":[data[1]["O"]],
                                               "C":[data[1]["C"]],"h":[data[1]["h"]],"l":[data[1]["l"]]
                                               ,"v":[data[1]["v"]],
                                               "qav":[data[1]["qav"]],"num_trades":[data[1]["num_trades"]],
                                               "taker_base_vol":[data[1]["taker_base_vol"]],"taker_quote_vol":[data[1]["taker_quote_vol"]]})
                        ramSeason = pd.concat((ramSeason, newDf), axis=0, join='outer')                  
                    if data[1]["Open_Time"] == minPrice[1]:
                        bearSaver = False
                        bearFinishIndex = data[0]
                maxPrice = [0]

                for extraRow in mainData.iterrows():
                    if extraRow[0] == bearFinishIndex:
                        extrabool = True
                    if extrabool:
                        if float(extraRow[1]["O"]) < float(minPrice[0]):                   
                            minPrice = [float(extraRow[1]["O"]), extraRow[1]["Open_Time"]]
                        if float(extraRow[1]["O"]) > float(maxPrice[0]):
                            maxPrice = [float(extraRow[1]["O"]), extraRow[1]["Open_Time"]]

                        distance = maxPrice[0] - minPrice[0]

                        if distance > maxPrice[0]*0.19:
                            if maxPrice[1] > minPrice[1]:
                                bearLastBreaker = True
                                print("AYI")
                                for i in mainData.iterrows():       # Kalıcı ayı sezonu tanımlanıyor.
                                    if i[1]["Open_Time"] == ramSeason.iloc[0]["Open_Time"]:
                                        saver = True
                                    if saver:
                                        newDf = pd.DataFrame({"Open_Time":[i[1]["Open_Time"]],"Close_Time":[i[1]["Close_Time"]],
                                                               "Price_Direction":[i[1]["Price_Direction"]],
                                                               "O":[i[1]["O"]],
                                                               "C":[i[1]["C"]],"h":[i[1]["h"]],"l":[i[1]["l"]]
                                                               ,"v":[i[1]["v"]],
                                                               "qav":[i[1]["qav"]],"num_trades":[i[1]["num_trades"]],
                                                               "taker_base_vol":[i[1]["taker_base_vol"]],"taker_quote_vol":[i[1]["taker_quote_vol"]]})
                                        season = pd.concat((season, newDf), axis=0, join='outer', ignore_index=True)                  
                                    if i[1]["Open_Time"] == minPrice[1]:
                                        saver = False
                                        with open("seasonCount.txt","r") as file:
                                            for i in file.readlines():
                                                seasonCount = i
                                        season.to_csv(("Seasons/"+"BearSeason---"+i+".csv").replace(":","."))

                                        with open("seasonCount.txt","w") as file:
                                            file.write(str(int(i)+1))
                                        
                                        

                                        for i in mainData.iterrows():
                                            if i[1]["Open_Time"] == minPrice[1]:
                                                deletedDf = mainData.iloc[i[0]:len(mainData)]                   
                                                deletedDf.reset_index(drop=True, inplace=True)
                                                deletedDf.to_csv("Price_Data/BTCUSDT_Data.csv")
                                                forBreaker = True
                                                break
                                                


                            else:
                                maxPrice = [0]

                            if bearLastBreaker:
                                break
        if forBreaker:
            break                            
