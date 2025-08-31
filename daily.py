#!/usr/bin/env python
#encoding:utf-8
#A股历史行情

import sqlite3
import akshare as ak
import datetime
import time

import a_all

def get_table_name(adjust=''):
    table = 'daily'
    if adjust == '':
        return table
    else:
        return table + '_' + adjust

def init_daily(conn, adjust=''):
    '''
    trading_date 交易日期; 2025-08-29
    open_price  开盘价; 单位: 分
    close_price 收盘价; 单位: 分
    high_price  最高价; 单位: 分
    low_price   最低价; 单位: 分
    trading_volume  成交量; 单位: 手
    trading_amount  成交额; 单位: 分
    amplitude       振幅=(当日最高价 - 当日最低价) / 前一个交易日收盘价; x100
    change_percent  涨跌幅=(当日收盘价 - 前一个交易日收盘价) / 前一个交易日收盘价; 单位: 分
    change_amount   涨跌额=当日收盘价 - 前一个交易日收盘价; 单位: 分
    outstanding_share   流动股本; 单位: 股
    turnover_tate   换手率=成交量/流动股本; x100
    '''

    table = get_table_name(adjust)

    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS %s (
        trading_date    TEXT NOT NULL,
        open_price      INTEGER NOT NULL,
        close_price     INTEGER NOT NULL,
        high_price      INTEGER NOT NULL,
        low_price       INTEGER NOT NULL,
        trading_volume  INTEGER NOT NULL,
        trading_amount  INTEGER NOT NULL,
        amplitude       INTEGER NOT NULL,
        change_percent  INTEGER NOT NULL,
        change_amount   INTEGER NOT NULL,
        outstanding_share   INTEGER NOT NULL,
        turnover_tate   INTEGER NOT NULL
    );
    ''' % (table, ))
    
    cur.execute('''
    CREATE UNIQUE INDEX IF NOT EXISTS uk_%s on %s (trading_date);
    ''' % (table, table))

    conn.commit()
    cur.close()

def f2i(fnum):
    return int(fnum * 100)

def save(adjust='', start_date='19700101', end_date='20250830'):
    table = get_table_name(adjust)

    all_data = a_all.read()
    for symbol, name in all_data.items():
        dup = symbol + '#' + adjust
        with open('data/dup_daily.txt', 'r') as fp:
            if fp.read().find(dup) >= 0:
                continue
        print('start', adjust, symbol, name)
        d = ak.stock_zh_a_hist(symbol=symbol[2:], period="daily", start_date=start_date, end_date=end_date, adjust=adjust)

        conn = sqlite3.connect('data/' + symbol + '/daily.db')
        init_daily(conn, adjust)

        cur = conn.cursor()
        for idx in d.index:
            trading_date = d['日期'][idx].strftime('%Y-%m-%d')
            row = (trading_date,
                f2i(d['开盘'][idx]), f2i(d['收盘'][idx]), f2i(d['最高'][idx]), f2i(d['最低'][idx]), int(d['成交量'][idx]), f2i(d['成交额'][idx]),
                f2i(d['振幅'][idx]), f2i(d['涨跌幅'][idx]), f2i(d['涨跌额'][idx]), 0, f2i(d['换手率'][idx]))
            print(row)
            sql = '''INSERT INTO %s (trading_date,
                open_price, close_price, high_price, low_price, trading_volume, trading_amount,
                amplitude, change_percent, change_amount, outstanding_share, turnover_tate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''' % (table, )
            cur.execute(sql, row)

        conn.commit()
        cur.close()
        conn.close()

        print('done', adjust, symbol, name)
        with open('data/dup_daily.txt', 'a') as fp:
            fp.write(dup + '\n')

        time.sleep(2)

if __name__ == '__main__':
    #end_date = datetime.date.today().strftime('%Y%m%d')
    end_date = '20250830'
    #save(end_date=end_date)
    save('qfq', end_date=end_date)
    #save('hfq', end_date=end_date)

