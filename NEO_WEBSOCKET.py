from jugaad_data.nse import NSELive
import pandas as pd
import time
import threading
import os


def dataframe(rawop):
  strike_price_holder = rawop['strikePrice'][0]
  calloi = cvol = putoi = pvol = 0
  data = []

  for i in range(len(rawop)):
    if (strike_price_holder == rawop['strikePrice'][i]):
      if (rawop['CE'][i]==0):
        calloi = calloi
      else:
        calloi = rawop['CE'][i]['openInterest'] + rawop['CE'][i]['changeinOpenInterest'] + calloi
        cvol = rawop['CE'][i]['totalTradedVolume'] + cvol
      if (rawop['PE'][i]==0):
        putoi = putoi
      else:
        putoi = rawop['PE'][i]['openInterest'] + rawop['PE'][i]['changeinOpenInterest'] + putoi
        pvol = rawop['PE'][i]['totalTradedVolume'] + pvol

    else:
      opdata = {'CALL OI' : calloi, 'CALL VOLUME':cvol,'STRIKE PRICE' : rawop['strikePrice'][i-1], 'PUT VOLUME': pvol, 'PUT OI' : putoi}
      data.append(opdata)

      calloi = cvol = putoi = pvol = 0
      strike_price_holder = rawop['strikePrice'][i]
      if (rawop['CE'][i]==0):
        calloi = calloi
      else:
        calloi = rawop['CE'][i]['openInterest'] + rawop['CE'][i]['changeinOpenInterest'] + calloi
        cvol = rawop['CE'][i]['totalTradedVolume'] + cvol
      if (rawop['PE'][i]==0):
        putoi = putoi
      else:
        putoi = rawop['PE'][i]['openInterest'] + rawop['PE'][i]['changeinOpenInterest'] + putoi
        pvol = rawop['PE'][i]['totalTradedVolume'] + pvol

  option_chain = pd.DataFrame(data)
  return option_chain

def get_option_chain(symbol):
  n = NSELive()
  df = pd.DataFrame.from_dict(n.equities_option_chain(symbol))   
  rawop = pd.DataFrame(df['records']['data']).fillna(0)  
  option_chain = dataframe(rawop)

  ATM = {}

  for strike_price in option_chain['STRIKE PRICE']:
    ATM[strike_price] = abs(df.at["underlyingValue","records"] - strike_price)

  ATM_RANGE = option_chain.loc[option_chain.loc[option_chain['STRIKE PRICE'] == min(ATM,key=ATM.get)].index[0]-5:option_chain.loc[option_chain['STRIKE PRICE'] == min(ATM,key=ATM.get)].index[0]+5].copy()
  ATM_RANGE.set_index('STRIKE PRICE',inplace=True)
  for strike_price in ATM_RANGE.index:
    try :
      if (ATM_RANGE.at[strike_price,"PUT OI"] != 0):
        ATM_RANGE.at[strike_price,"Seller"] = round(ATM_RANGE.at[strike_price,"CALL OI"]/ATM_RANGE.at[strike_price,"PUT OI"],2)
      if (ATM_RANGE.at[strike_price,"CALL OI"] !=0):
        ATM_RANGE.at[strike_price,"Buyer"] = round(ATM_RANGE.at[strike_price,"PUT OI"]/ATM_RANGE.at[strike_price,"CALL OI"],2)
    except: continue

  gap = " "*3
  if (ATM_RANGE.at[min(ATM,key=ATM.get),"Buyer"] >= 5) and ((ATM_RANGE.at[min(ATM,key=ATM.get),"PUT OI"] - ATM_RANGE.at[min(ATM,key=ATM.get),"CALL OI"]) >= 100):
    time_stamp = str(df.at["timestamp","records"])[-8:]
    price = str(round(df.at["underlyingValue","records"],2))
    OI_DIFF = str(ATM_RANGE.at[min(ATM,key=ATM.get),"PUT OI"] - ATM_RANGE.at[min(ATM,key=ATM.get),"CALL OI"])
    STRENGTH = str(ATM_RANGE.at[min(ATM,key=ATM.get),"Buyer"]) 
    BUY = '\033[92m\033[1mBUY\033[0m'   
    print_data = f"{symbol:20s}{gap}{time_stamp:13s}{gap}{gap}{price:10s}{gap}{gap}{str(BUY):11s}{' '}{gap}{gap}{gap}{OI_DIFF:10s}{gap}{STRENGTH:10s}"
    print(print_data,"\n")    

  if (ATM_RANGE.at[min(ATM,key=ATM.get),"Seller"] >= 5) and ((ATM_RANGE.at[min(ATM,key=ATM.get),"CALL OI"] - ATM_RANGE.at[min(ATM,key=ATM.get),"PUT OI"]) >= 100):
    time_stamp = str(df.at["timestamp","records"])[-8:]
    price = str(round(df.at["underlyingValue","records"],2))
    OI_DIFF = str(ATM_RANGE.at[min(ATM,key=ATM.get),"CALL OI"] - ATM_RANGE.at[min(ATM,key=ATM.get),"PUT OI"])
    STRENGTH = str(ATM_RANGE.at[min(ATM,key=ATM.get),"Seller"])        
    SELL = '\033[91m\033[1mSELL\033[0m'
    print_data = f"{symbol:20s}{gap}{time_stamp:13s}{gap}{gap}{price:10s}{gap}{gap}{str(SELL):10s}{gap}{gap}{gap}{OI_DIFF:10s}{gap}{STRENGTH:10s}"
    print(print_data,"\n")

  

options_stocks = pd.read_csv("OPTIONS.csv")

gap = " "*3
heading = f"{'Stock':22s}{gap}{'Time':11s}{gap}{gap}{'Price':11s}{gap}{'Buy/Sell':10s}{gap}{'OI DIFF':10s}{gap}{'STRENGTH':10s}"


while True:

  print("="*90)
  print(heading)
  print("-"*90,"\n")

  start = time.perf_counter()
  for stocks in options_stocks["Symbol"]:
      threading.Thread(target=get_option_chain(stocks)).start()      
      
  stop = time.perf_counter()
  print("-"*90,"\n")
  print("time taken:", stop - start)
  time.sleep(60)
  os.system("cls")





