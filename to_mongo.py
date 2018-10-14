# -*- coding: utf-8 -*-
"""
Created on Sat Jul 21 21:44:42 2018

@author: teng
"""
import sys
#sys.path.append('C:\Users\teng\Anaconda3\Lib')
import pusherclient 
import logging
import time
import datetime
import pandas as pd
import json
import numpy as np
import pymongo
from pymongo import *

def disconnect():
    pusher.unsubscribe("live_trades_ethusd")
    pusher.unsubscribe("live_orders_ethusd")
    pusher.unsubscribe("order_books_ethusd")
    pusher.disconnect()
    

mongodb_host = 'localhost'
mongodb_port = '27017'

client = pymongo.MongoClient(mongodb_host+':'+mongodb_port)
db = client.ETHUSD
df_orders = pd.DataFrame(columns = ['MTS','EVENT','ACTION','PRICE','AMOUNT','ID'])
df_trades = pd.DataFrame(columns = ['MTS','ACTION','PRICE','AMOUNT','ID','BUY_ORDER_ID','SELL_ORDER_ID'])
df_ob = pd.DataFrame(columns = ['MTS','BIDS','ASKS'])

def microtimestamp(data):
    microtimestamp = data['microtimestamp']
    mille_sec = (int(microtimestamp))/1000000
    formatted_time = (datetime.datetime.utcfromtimestamp(mille_sec).strftime('%Y-%m-%d %H:%M:%S.%f'))
    return formatted_time

def timestamp(data):
    timestamp = data['timestamp']
    mille_sec = (int(timestamp))/1
    formatted_time = (datetime.datetime.utcfromtimestamp(mille_sec).strftime('%Y-%m-%d %H:%M:%S.%f'))
    return formatted_time

def buyorsell(x):
    if x == 0:
        return 'Buy'
    else:
        return 'Sell'
   

def trade_callback(data): 
    data = json.loads(data)
    global df_trades
    df_trades = df_trades.append({'MTS':timestamp(data),
                    'ACTION':'BUY' if data['type']==0 else 'SELL',
                    'PRICE':data['price'],
                    'AMOUNT':data['amount'],
                    'ID':int(data['id']),
                    'BUY_ORDER_ID':data['buy_order_id'],
                    'SELL_ORDER_ID':data['sell_order_id']},ignore_index=True)
    global db
    db.live_trades.insert_one({'MTS':timestamp(data),
                    'ACTION':'BUY' if data['type']==0 else 'SELL',
                    'PRICE':data['price'],
                    'AMOUNT':data['amount'],
                    'ID':int(data['id']),
                    'BUY_ORDER_ID':data['buy_order_id'],
                    'SELL_ORDER_ID':data['sell_order_id']})
#    

def order_deleted_callback(data):
    data = json.loads(data)

    global df_orders
    df_orders = df_orders.append({'MTS':microtimestamp(data),
                                  'EVENT':'Deleted',
                                  'ACTION':'Buy' if data[
            'order_type']==0 else 'Sell',
                                  'PRICE':data['price'],
                                  'AMOUNT':data['amount'],
                                  'ID':data['id']},ignore_index=True)
    global db
    db.live_orders.insert_one({'MTS':microtimestamp(data),
                                  'EVENT':'Deleted',
                                  'ACTION':'Buy' if data[
            'order_type']==0 else 'Sell',
                                  'PRICE':data['price'],
                                  'AMOUNT':data['amount'],
                                  'ID':data['id']})
    
    
    
def order_created_callback(data):
    data = json.loads(data)
    global df_orders
    df_orders = df_orders.append({'MTS':microtimestamp(data),
                                  'EVENT':'Created',
                                  'ACTION':'Buy' if data[
            'order_type']==0 else 'Sell',
                                  'PRICE':data['price'],
                                  'AMOUNT':data['amount'],
                                  'ID':data['id']},ignore_index=True)
    global db
    db.live_orders.insert_one({'MTS':microtimestamp(data),
                                  'EVENT':'Created',
                                  'ACTION':'Buy' if data[
            'order_type']==0 else 'Sell',
                                  'PRICE':data['price'],
                                  'AMOUNT':data['amount'],
                                  'ID':data['id']})

def order_changed_callback(data):
    data = json.loads(data)
    global df_orders 
    df_orders = df_orders.append({'MTS':microtimestamp(data),
                                  'EVENT':'Changed',
                                  'ACTION':'Buy' if data[
            'order_type']==0 else 'Sell',
                                  'PRICE':data['price'],
                                  'AMOUNT':data['amount'],
                                  'ID':data['id']},ignore_index=True)
    global db
    db.live_orders.insert_one({'MTS':microtimestamp(data),
                                  'EVENT':'Changed',
                                  'ACTION':'Buy' if data[
            'order_type']==0 else 'Sell',
                                  'PRICE':data['price'],
                                  'AMOUNT':data['amount'],
                                  'ID':data['id']})
    

def order_book_callback(data):
    data = json.loads(data)
    global df_ob
    df_ob=df_ob.append({'MTS':timestamp(data),
                        'BIDS':data['bids'],
                        'ASKS':data['asks']},ignore_index=True)
    global db
    db.orderbooks.insert_one({'MTS':timestamp(data),
                        'BIDS':data['bids'],
                        'ASKS':data['asks']})
#   
    
    
def connect_handler(data): #this gets called when the Pusher connection is established
    trades_channel = pusher.subscribe("live_trades_ethusd")
    trades_channel.bind('trade', trade_callback)

    order_book_channel = pusher.subscribe('order_book_ethusd');
    order_book_channel.bind('data', order_book_callback)
    
    orders_channel = pusher.subscribe("live_orders_ethusd")
    orders_channel.bind('order_deleted', order_deleted_callback)
    orders_channel.bind('order_created', order_created_callback)
    orders_channel.bind('order_changed', order_changed_callback)



if __name__ == '__main__':
    pusher = pusherclient.Pusher("de504dc5763aeef9ff52")
    pusher.connection.logger.setLevel(logging.WARNING) #no need for this line if you want everything printed out by the logger
    pusher.connection.bind('pusher:connection_established', connect_handler)
    pusher.connect()


    while True:  #run until ctrl+c interrupts
        time.sleep(1)
