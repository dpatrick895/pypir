import sqlite3
from datetime import datetime
from pycoingecko import CoinGeckoAPI
import pandas as pd
import yfinance as yf

class portfolio:

    def crypto(self, currency='aud'):

        con = sqlite3.connect(self.dbName)
        cur = con.cursor()

        coinList = cur.execute("SELECT GROUP_CONCAT(Asset,',') FROM ASSETS where type ='Crypto'").fetchone()[0]
        if(coinList is None):
            return
        cryptHoldings = pd.read_sql('Select * from Holdings where type = "Crypto"', con)
        #date = datetime.today().strftime('%Y-%m-%d')

        cg = CoinGeckoAPI()

        holdings = cg.get_price(ids=coinList, vs_currencies=currency)
        holdings = pd.DataFrame.from_dict(holdings).rename(index={'aud': 'Price'}).T
        holdings = holdings.reset_index().rename(columns={"index": "Asset"})
        holdings = holdings.merge(cryptHoldings, on='Asset')
        holdings['Exposure'] = holdings['Price']*holdings['Quantity']
              
        return holdings

    def getStockData(self,ticker, period='1d', priceOnly = 'Y'):

        """This function can be utilised for both 
        backtesting and daily price updates"""
    
        stockDF = pd.DataFrame()
        stock = yf.download(ticker, period=period).reset_index()
        stockDF['Date'],stockDF['Asset'],stockDF['Price'] \
            = [stock['Date'],ticker,stock['Close']]

        if priceOnly == 'Y':
            return stock['Close'].iloc[0]
        return stockDF

    def stockPortfolio(self):

        con = sqlite3.connect(self.dbName)
        stocks = pd.read_sql('select * from holdings where type = "Stocks"',con)
        for i in range(len(stocks)):
            stockPrice = self.getStockData(stocks.at[i, 'Asset'])
            stocks.at[i, 'Price'] = stockPrice
            stocks.at[i, 'Exposure'] = stocks.at[i, 'Price']*stocks.at[i, 'Quantity']
        
        return stocks

    def getHoldings(self):
        con = sqlite3.connect(self.dbName)
        stocks = pd.read_sql('select * from holdings',con)
        con.close()
        return stocks

    def nonAPIHoldings(self):
        con = sqlite3.connect(self.dbName)
        data = pd.read_sql("""SELECT H.AssetID,H.Asset,H.Type,H.Quantity,P.Price FROM Holdings H
                INNER JOIN (SELECT MAX(PriceID) as PriceID,AssetID FROM Prices group by AssetID) T
                ON H.AssetID=T.AssetID INNER JOIN Prices P  ON P.PriceID=T.PriceID""",con)
        data['Exposure'] = data['Price']*data['Quantity']
        con.close()
        return data
    
    def portValuation(self):

        portToDb = pd.DataFrame()
        ## call to list of assets to 
        port = self.stockPortfolio()
        crypt = self.crypto()
        if (crypt is not None):
            port = port.append(crypt, ignore_index=True)
        ## apply pattern for each additional asset type
        ## add metals 
        ## add cash
        nonAPI = self.nonAPIHoldings()
        if (nonAPI is not None):
            port = port.append(nonAPI, ignore_index=True)

        portToDb['AssetID'] = port['AssetID']
        portToDb['Exposure'] = port['Exposure']
        portToDb['Date'] = datetime.today().strftime("%m/%d/%Y %H:%M:%S")
        con = sqlite3.connect(self.dbName)
        cur = con.cursor()

        runID = cur.execute("SELECT MAX(RunID) FROM Portfolio ").fetchone()[0]
        if runID is None:
            runID =0
        portToDb['RunID']=runID+1

        for index, row in portToDb.iterrows():
            cur.execute("INSERT INTO Portfolio (RunID,AssetID,Exposure,Date) values(?,?,?,?)", (row.RunID, row.AssetID, row.Exposure,row.Date))

        con.commit()
        con.close()

        return portToDb

    

    def __init__(self,dbName='portfolio.db'):
        self.dbName = dbName

