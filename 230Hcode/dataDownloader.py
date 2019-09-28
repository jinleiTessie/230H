#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 16:27:42 2019

@author: jinlei
"""
from datetime import date, timedelta
import datetime
import pandas as pd


class DataStore:
    def __init__(self,ticker_dates,tickers,freq):
        self.freq        = freq
        self.resampledData={}
        self.tickers=tickers
        self.tickerDates=ticker_dates
    
    def loadData(self, tag, ticker):
        path=path='data/'+tag+'/'+ticker+'.csv'
        df = pd.read_csv(path,error_bad_lines=False)
        df=df[df.DATE.isin(self.tickerDates[ticker])]
        df["timestamp"]=df.apply(lambda row: pd.to_datetime(str(row['DATE']) + ' ' + str(row['TIME_M'])), axis=1)
        df=df.drop(columns=["DATE","TIME_M"])
        return df
    
    def loadTradeData(self,ticker):
        return self.loadData("trade", ticker)
    
    def loadQuoteData(self,ticker):
        return self.loadData("quote",ticker)
    
    def resampleQuote(self, df, frequency):
        print ("resampling quote data to ",frequency)
        df=df.rename(columns={'BID':'bidPrice','ASK':'askPrice','BIDSIZ':'bidSizeBalance','ASKSIZ':'askSizeBalance',
                              'QU_SEQNUM':'quote_sequential_number','SYM_ROOT':'symbol'})[['symbol',"timestamp","bidPrice",'askPrice','bidSizeBalance','askSizeBalance','quote_sequential_number']]
        df=df.sort_values('timestamp').groupby(['symbol']).resample(frequency,on="timestamp").agg({'bidSizeBalance' : ["min","max","last"],
                      'askSizeBalance' : ["min","max","last"],'bidPrice' : ["min","max","last"],'askPrice' : ["min","max","last"], 'timestamp':"last", "quote_sequential_number":"size"}).reset_index()
        df.columns = df.columns.map('_'.join)
        df = df.reset_index().rename(columns={'symbol_':'symbol',"timestamp_":"timestamp","timestamp_last":"timestamp_lastQuote"}).drop(columns=['index'])
        df=df.dropna()
        return df
    
    def resampleTrade(self, df, frequency):
        print ("resampling trade data to ",frequency)
        df=df.rename(columns={'SYM_ROOT':'symbol','PRICE':'tradePrice','SIZE':'tradeSize','TR_SEQNUM':'trade_sequential_number'})[["timestamp","symbol","tradeSize","tradePrice", "trade_sequential_number"]]
        df=df.sort_values('timestamp').groupby(['symbol']).resample(frequency,on="timestamp").agg({'tradeSize' : ["min","max","last"],
                      'tradePrice' : ["min","max","last","mean"], 'timestamp':"last","trade_sequential_number":"size"}).reset_index()
        df.columns = df.columns.map('_'.join)
        df = df.reset_index().rename(columns={'symbol_':'symbol',"timestamp_":"timestamp","timestamp_last":"timestamp_lastTrade"}).drop(columns=['index'])
        df=df.dropna()
        return df

    def resampleData(self):
        for ticker in self.tickers:
            if ticker not in (self.resampledData.keys()):
                self.resampledData[ticker]={}
                quote=self.loadQuoteData(ticker)
                trade=self.loadTradeData(ticker)
                for frequency in self.freq:
                    if frequency not in (self.resampledData[ticker].keys()):
                        quote_resampled=self.resampleQuote(quote,frequency)
                        trade_resampled=self.resampleTrade(trade,frequency)
                        self.resampledData[ticker][frequency] = pd.merge(quote_resampled,trade_resampled, on=["symbol", "timestamp"], how="left")
        return self.resampledData
