#!/usr/bin/env python
#encoding:utf-8
#A股历史行情

import sqlite3
import akshare as ak
import datetime
import time

import a_all

def get_table_name(adjust=''):
    table = 'a_daily'
    if adjust == '':
        return table
    else:
        return table + '_' + adjust

def init_a_daily(conn, adjust=''):
    table = get_table_name(adjust)

    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS %s (
        symbol          TEXT NOT NULL,
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
    CREATE UNIQUE INDEX IF NOT EXISTS uk_%s on %s (symbol, trading_date);
    ''' % (table, table))

    conn.commit()
    cur.close()

def f2i(fnum):
    return int(fnum * 100)

def save(adjust='', start_date='19700101', end_date='20250830'):
    table = get_table_name(adjust)

    all_data = a_all.read()
    for symbol, name in all_data.items():
        with open('daily.txt', 'r') as fp:
            if fp.read().find(symbol + adjust) >= 0:
                continue
        print('start', adjust, symbol, name)
        d = ak.stock_zh_a_hist(symbol=symbol[2:], period="daily", start_date=start_date, end_date=end_date, adjust=adjust)

        conn = sqlite3.connect('data/a_daily.db')
        cur = conn.cursor()

        for idx in d.index:
            trading_date = d['日期'][idx].strftime('%Y-%m-%d')
            row = (symbol[2:], trading_date,
                f2i(d['开盘'][idx]), f2i(d['收盘'][idx]), f2i(d['最高'][idx]), f2i(d['最低'][idx]), int(d['成交量'][idx]), f2i(d['成交额'][idx]),
                f2i(d['振幅'][idx]), f2i(d['涨跌幅'][idx]), f2i(d['涨跌额'][idx]), 0, f2i(d['换手率'][idx]))
            print(row)
            sql = '''INSERT INTO %s (symbol, trading_date,
                open_price, close_price, high_price, low_price, trading_volume, trading_amount,
                amplitude, change_percent, change_amount, outstanding_share, turnover_tate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''' % (table, )
            cur.execute(sql, row)

        conn.commit()
        cur.close()
        conn.close()

        print('done', adjust, symbol, name)
        with open('daily.txt', 'a+') as fp:
            fp.write(symbol + adjust + '\n')

        time.sleep(1)

if __name__ == '__main__':
    conn = sqlite3.connect('data/a_daily.db')
    init_a_daily(conn) # 不复权
    init_a_daily(conn, 'qfq') # 前复权
    init_a_daily(conn, 'hfq') # 后复权
    conn.close()

    #end_date = datetime.date.today().strftime('%Y%m%d')
    end_date = '20250830'
    #save(end_date=end_date)
    save('qfq', end_date=end_date)
    #save('hfq', end_date=end_date)

