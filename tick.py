#!/usr/bin/env python
#encoding:utf-8
#A股历史行情

import sys
import sqlite3
import akshare as ak
from datetime import datetime
import time

import a_all

def init_tick(conn):
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tick (
        trading_time    INTEGER NOT NULL,
        trading_price   INTEGER NOT NULL,
        change_price    INTEGER NOT NULL,
        trading_volume  INTEGER NOT NULL,
        trading_amount  INTEGER NOT NULL,
        trading_nature  INTEGER NOT NULL
    );
    ''')
    
    cur.execute('''
    CREATE INDEX IF NOT EXISTS ik_tick on tick (trading_time);
    ''')

    conn.commit()
    cur.close()

def f2i(fnum):
    return int(fnum * 100)

def save(trading_date):
    all_data = a_all.read()
    for symbol, name in all_data.items():
        dup = symbol + '#' + trading_date
        with open('data/dup_tick.txt', 'r') as fp:
            if fp.read().find(dup) >= 0:
                continue
        print('start', symbol, name)

        d = ak.stock_zh_a_tick_tx_js(symbol=symbol)

        conn = sqlite3.connect('data/' + symbol + '/tick.db')
        init_tick(conn)

        cur = conn.cursor()
        for idx in d.index:
            nature = d['性质'][idx]
            if nature == '买盘':
                trading_nature = 1
            elif nature == '卖盘':
                trading_nature = -1
            elif nature == '中性盘':
                trading_nature = 0
            else:
                print('unknown nature', nature)
                sys.exit(-1)

            trading_time = int(datetime.strptime(trading_date + ' ' + d['成交时间'][idx], '%Y-%m-%d %H:%M:%S').timestamp())
            row = (trading_time, f2i(d['成交价格'][idx]), f2i(d['价格变动'][idx]), int(d['成交量'][idx]), f2i(d['成交金额'][idx]), trading_nature)
            print(row)
            sql = '''INSERT INTO tick (trading_time, trading_price, change_price, trading_volume, trading_amount, trading_nature) VALUES (?, ?, ?, ?, ?, ?)'''
            cur.execute(sql, row)

        conn.commit()
        cur.close()
        conn.close()

        print('done', symbol, name)
        with open('data/dup_tick.txt', 'a') as fp:
            fp.write(dup + '\n')

if __name__ == '__main__':
    trading_date = '2025-08-29'
    save(trading_date)
