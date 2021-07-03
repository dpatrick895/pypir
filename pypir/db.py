from os import path
import sqlite3
from datetime import datetime
import pandas as pd


class Db:

    def insert_asset(self):

        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        print("Asset Name:")
        asset = input()
        print("Asset type:")
        type = input()

        cur.execute("Insert INTO Assets (Asset,Type) Values (?,?)",(asset,type))

        con.commit()
        con.close()

    def insert_price(self):
        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        print("Asset Name:")
        asset = input()
        print("Asset type:")
        type = input()
        asset = cur.execute("SELECT AssetID From Assets WHERE Asset = ? AND Type = ?",(asset,type)).fetchone()
        if (asset is None):
            print("asset does not exist")
            con.commit()
            con.close()
            return 
            
        else:
            assetID = asset[0]

        print("Price")
        price = input()

        cur.execute("Insert INTO Prices(AssetID,Price) Values (?,?)",(assetID,price))

        con.commit()
        con.close()

    def insert_trade(self, shortDate = None):
        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        print("Asset Name:")
        asset = input()
        print("Asset type:")
        type = input()
        asset = cur.execute("SELECT AssetID From Assets WHERE Asset = ? AND Type = ?",(asset,type)).fetchone()
        if (asset is None):
            print("asset does not exist")
            con.commit()
            con.close()
            return 
            
        else:
            assetID = asset[0]

        print("Purchased Quantity:")
        quantity = input()
        print("Purchase price:")
        price = input()
        print("transaction type")
        transType = input()

        if (shortDate is None):
            shortDate = datetime.today().strftime("%d/%m/%Y")
        
        cur.execute("Insert INTO Trades (AssetID,Quantity,Price,ShortDate,TransactionType) Values (?,?,?,?,?)",(assetID,quantity,price,shortDate,transType))

        con.commit()
        con.close()

    def seed_db(self, sheetname, repopulate='N'):
        "Seeds the database with trade history"
        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        assets = pd.read_excel(sheetname,'Assets')
        trades = pd.read_excel(sheetname, 'Trades')

        
        trades['ShortDate'] = trades['ShortDate'].astype(str)

        for index,row in assets.iterrows():
            cur.execute("INSERT INTO Assets (Asset,Type) values (?,?)",(row.Asset,row.Type))


        con.commit()

        for index,row in trades.iterrows():
            assetID = cur.execute("SELECT AssetID From Assets WHERE Asset = ? AND Type = ?",(row.Asset,row.Type)).fetchone()[0]
            cur.execute("INSERT INTO Trades (AssetID,Quantity,Price,ShortDate,TransactionType) values (?,?,?,?,?)",(assetID,row.Quantity,row.Price,row.ShortDate,row.TransactionType))
        
        con.commit()
        con.close()


    def create_database(self,dbName):

        con = sqlite3.connect(dbName)
        cur = con.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS Assets (AssetID INTEGER, Asset TEXT, Type TEXT, Primary Key(AssetID AutoIncrement))''')

        cur.execute('''CREATE TABLE IF NOT EXISTS Portfolio
            (RunID integer, AssetID Integer, Exposure Numeric, Date Text)''')

        cur.execute('''CREATE TABLE IF NOT EXISTS Trades
            (TradeID integer, AssetID Integer, Quantity Numeric, Price Numeric,ShortDate Text, TransactionType Text,Primary Key(TradeID AutoIncrement))''')

        cur.execute('''CREATE TABLE IF NOT EXISTS Prices (PriceID INTEGER, AssetID INTEGER, Price Numeric, Primary Key(PriceID AutoIncrement))''')

        cur.execute('''CREATE VIEW IF NOT EXISTS Holdings AS
            SELECT a.AssetID,a.Asset,a.type,SUM(t.Quantity) as Quantity FROM 
            Assets a inner join trades t 
            on a.AssetID = T.AssetID
            group by a.AssetID, a.Asset, a.Type''')

        cur.execute('''CREATE VIEW IF NOT EXISTS Valuation
            AS SELECT A.Asset,A.Type,P.Exposure,P.Date
            FROM Portfolio  P INNER JOIN Assets A ON P.AssetID = A.AssetID
            WHERE RunID in (SELECT MAX(RunID) FROM Portfolio)''')

        con.commit()

        con.close()

        print("Database Created")

    def __init__(self,dbName='portfolio.db'):

        if path.isfile(dbName):
            print("Db exists")
        else:
            print("Creating DB")
            self.create_database(dbName)
        self.dbName = dbName

    





        