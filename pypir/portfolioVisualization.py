import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pandas.io import sql

class portViz:

    def byAsset(self):

        "Bar graph of each holding in the portfolio"
        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        assets = pd.read_sql("SELECT * FROM Valuation order by exposure desc",con)
        plt.figure(figsize=(14,8))
        plt.bar(assets['Asset'],assets['Exposure'])
        con.close()
        return plt.show()

    def byAssetClass(self):

        """Pie graph of the portfolio broken down by asset type"""
        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        assets = pd.read_sql("SELECT SUM(Exposure) as Exposure,Type FROM Valuation GROUP BY TYPE",con)
        fig = plt.figure()
        ax = fig.add_axes([0,0,1,1])
        ax.axis('equal')
        ax.pie(assets['Exposure'],labels=assets['Type'],autopct='%1.2f%%')
        con.close()
        return plt.show()

    def assetTypeWeightings(self, type='Stocks'):
        """Produces a pie graph broken down by type"""
        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        sql = "Select Exposure, Asset from Valuation WHERE type = '" + type + "'"
        assets = pd.read_sql(sql,con)
        fig = plt.figure()
        ax = fig.add_axes([0,0,1,1])
        ax.axis('equal')
        ax.pie(assets['Exposure'],labels=assets['Asset'],autopct='%1.2f%%')
        con.close()
        return plt.show()


    def portfolioValuation(self):

        """Shows a line graph of the portfolio value over time"""

        con = sqlite3.connect(self.dbName)
        cur = con.cursor()
        assets = pd.read_sql("SELECT Sum(exposure) as Value, Date FROM Portfolio GROUP BY RunID",con)
        assets['Date'] = pd.to_datetime(assets['Date'])
        plt.figure(figsize=(16,12))
        plt.plot(assets['Date'], assets['Value'], color='blue')
        plt.title('Asset value', fontsize=14)
        plt.xlabel('Time', fontsize=14)
        plt.ylabel('Value', fontsize=14)
        plt.grid(True)
        plt.show()
        con.close()
        return plt.show()

    def __init__(self, dbName='portfolio.db'):
        self.dbName = dbName

