#!/usr/bin/env python
#encoding:utf-8
#A股资金流向

import os
import sys
import sqlite3
import akshare as ak
from datetime import datetime
import time

import a_all

def init_zjlx(conn):
    '''
    trading_date    交易日期; 2025-08-29
    super_net_buy   超大单净买入; 单位: 分
    big_net_buy     超大单净买入; 单位: 分
    middle_net_buy  超大单净买入; 单位: 分
    small_net_buy   超大单净买入; 单位: 分
    '''

    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS zjlx (
        trading_date    TEXT NOT NULL,
        super_net_buy   INTEGER NOT NULL,
        big_net_buy     INTEGER NOT NULL,
        middle_net_buy  INTEGER NOT NULL,
        small_net_buy   INTEGER NOT NULL,
        net_buy         INTEGER NOT NULL
    );
    ''')
    
    cur.execute('''
    CREATE INDEX IF NOT EXISTS ik_zjlx on zjlx (trading_date);
    ''')

    conn.commit()
    cur.close()

def f2i(fnum):
    return int(fnum * 100)

def save():
    dup_file = 'data/dup_zjlx.txt'
    if not os.path.exists(dup_file):
        with open(dup_file, 'w') as fp:
            pass

    all_data = a_all.read()
    for symbol, name in all_data.items():
        dup = symbol
        with open(dup_file, 'r') as fp:
            if fp.read().find(dup) >= 0:
                continue

        print('start', symbol, name)

        d = ak.stock_individual_fund_flow(stock=symbol[2:], market=symbol[:2])

        conn = sqlite3.connect('data/' + symbol + '/zjlx.db')
        init_zjlx(conn)

        cur = conn.cursor()
        for idx in d.index:
            trading_date = d['日期'][idx].strftime('%Y-%m-%d')
            super_net_buy = f2i(d['超大单净流入-净额'][idx])
            big_net_buy = f2i(d['大单净流入-净额'][idx])
            middle_net_buy = f2i(d['中单净流入-净额'][idx])
            small_net_buy = f2i(d['小单净流入-净额'][idx])
            row = (trading_date, super_net_buy, big_net_buy, middle_net_buy, small_net_buy)
            print(row)
            sql = '''INSERT INTO zjlx (trading_date, super_net_buy, big_net_buy, middle_net_buy, small_net_buy) VALUES (?, ?, ?, ?, ?)'''
            try:
                cur.execute(sql, row)
            except sqlite3.IntegrityError:
                continue

        conn.commit()
        cur.close()
        conn.close()

        print('done', symbol, name)
        with open(dup_file, 'a') as fp:
            fp.write(dup + '\n')

        time.sleep(2)

if __name__ == '__main__':
    save()
