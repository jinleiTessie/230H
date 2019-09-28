#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 18:20:27 2019

@author: jinlei
"""
import dataDownloader as db
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
pd.set_option('display.expand_frame_repr', False)    
import numpy as np
import seaborn as sns

def plot_corr_heatmap(X, title="Corr Heatmap"):
    X_corr = X.corr()
    plt.figure(figsize=(16,10))
    plt.title(title)
    mask = np.zeros_like(X_corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True
    cmap = sns.diverging_palette(220, 10, as_cmap=True)
    sns.heatmap(mask=mask, data=X_corr, annot=True, cmap=cmap)

def pairplot_with_target(X,frequency ,features, target):
    def pairplot(x, y, **kwargs):
        ax = plt.gca()
        ts = pd.DataFrame({'x': x, 'y': y})
        ts=ts.dropna()
        ts.plot.scatter(x="x",y="y",ax=ax)
        plt.xticks(rotation=90)
    f = pd.melt(X, id_vars=[target], value_vars=features)
    g = sns.FacetGrid(f, col="variable",  col_wrap=3, sharex=False, sharey=False, size=3)
    g = g.map(pairplot, "value", target)
    g.fig.subplots_adjust(top=0.9)
    g.fig.suptitle("scatter plot feature vs. return, interval="+frequency+" minutes")

def plot_timeseries(X, ticker, freq):
    plt.figure(figsize=(10,6))
    plt.title('high frequency trading activities for ticker '+ticker+" for "+freq)
    sns.lineplot(X.timestamp, X.hft_activity, label='HFT')
    plt.xlabel('time')
    plt.ylabel('oder/trade ratio')
    plt.show()
#%%config
frequency_list=["1T","60T","1D"]#1T: 1minute, 60T:1h, 1D:1day
ticker_dates={"MSFT":["20180105","20181031"], #pls change 
            "AAPL":["20180105","20180105"]}
#             
datastore = db.DataStore(ticker_dates, ticker_dates.keys(),frequency_list)
data=datastore.resampleData()
#%%feature engineer
for ticker in data.keys():
    for freq in data[ticker].keys():
        df=data[ticker][freq]    
        #make features
        df["hft_activity"]=df["quote_sequential_number_size"]/df["trade_sequential_number_size"]
        df['return'] = df.sort_values('timestamp').groupby(['symbol'])["tradePrice_last"].pct_change(1)
        df["bidAskSpread"]=(df["bidPrice_last"]-df["askPrice_last"])
        df['tradePriceVolatility'] = np.log(df["tradePrice_max"])-np.log(df["tradePrice_min"])
        plot_timeseries(df[["timestamp","hft_activity"]],ticker, freq)
        if freq!="1D": #pls change when we have multiple day data
            plot_corr_heatmap(df.corr(),title="Corr Heatmap for interval="+freq+" minutes")
            pairplot_with_target(df,freq, list(set(df.columns) - set(['hft_activity','symbol','timestamp_lastQuote','timestamp','timestamp_lastTrade'])), 'hft_activity')
