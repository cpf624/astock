#!/usr/bin/env python
#encoding:utf-8
#A股融资融券详情

import os
import time
import json
import requests
import sqlite3

import a_all

def init_rzrq(conn):
    '''
    trading_date    交易日期; 2025-08-29
    capitalization  市值; 单位: 分
    rz_balance      融资余额; 单位: 分
    rz_buy          融资买入额; 单位: 分
    rz_redeem       融资偿还额; 单位: 分
    rz_net_buy      融资净买入; 单位: 分
    rq_balance      融券余额; 单位: 分
    rq_remain       融券余量; 单位: 股
    rq_sell         融券卖出量; 单位: 股
    rq_redeem       融券偿还量; 单位: 股
    rq_net_sell     融券净卖出量; 单位: 股
    '''

    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS rzrq (
        trading_date    TEXT NOT NULL,
        capitalization  INTEGER NOT NULL,
        rz_balance      INTEGER NOT NULL,
        rz_buy          INTEGER NOT NULL,
        rz_redeem       INTEGER NOT NULL,
        rz_net_buy      INTEGER NOT NULL,
        rq_balance      INTEGER NOT NULL,
        rq_remain       INTEGER NOT NULL,
        rq_sell         INTEGER NOT NULL,
        rq_redeem       INTEGER NOT NULL,
        rq_net_sell     INTEGER NOT NULL
    );
    ''')
    
    cur.execute('''
    CREATE UNIQUE INDEX IF NOT EXISTS uk_rzrq on rzrq (trading_date);
    ''')

    conn.commit()
    cur.close()

def download(symbol, page_num):
    symbol = symbol[2:]
    url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPTA_WEB_RZRQ_GGMX&columns=ALL&source=WEB&sortColumns=date&sortTypes=-1&pageNumber=' + str(page_num) + '&pageSize=50&filter=(scode%3D"' + symbol + '")&_=1757234633768'
    resp = requests.get(url)
    return json.loads(resp.text)
        

def save():
    dup_file = 'data/dup_rzrq.txt'
    if not os.path.exists(dup_file):
        with open(dup_file, 'w') as fp:
            pass

    all_data = a_all.read()
    for symbol, name in all_data.items():
        dup = symbol
        with open(dup_file, 'r') as fp:
            if fp.read().find(dup) >= 0:
                continue

        conn = sqlite3.connect('data/' + symbol + '/rzrq.db')
        init_rzrq(conn)
        cur = conn.cursor()

        print('start', symbol, name)
        page_num = 1
        while True:
            rd = download(symbol, page_num)
            if not rd or not rd['result'] or not rd['result']['data']:
                break
            page_num += 1

            has_dup = False
            for d in rd['result']['data']:
                trading_date = d['DATE'][0:10]
                capitalization = int(d['SZ']) * 100 # 市值
                rz_balance = int(d['RZYE']) * 100 # 融资余额
                rz_buy = int(d['RZMRE']) * 100 # 融资买入额
                rz_redeem = int(d['RZCHE']) * 100 # 融资偿还额
                rz_net_buy = int(d['RZJME']) * 100 # 融资偿还额
                rq_balance = int(d['RQYE']) * 100 # 融券余额
                rq_remain = int(d['RQYL']) # 融券余量
                rq_sell = int(d['RQMCL']) # 融券卖出量
                rq_redeem = int(d['RQCHL']) # 融券偿还量
                rq_net_sell = int(d['RQJMG']) # 融券净卖出量
                row = (trading_date, capitalization, rz_balance, rz_buy, rz_redeem, rz_net_buy, rq_balance, rq_remain, rq_sell, rq_redeem, rq_net_sell)
                print(row)

                sql = '''INSERT INTO rzrq (trading_date, capitalization, rz_balance, rz_buy, rz_redeem, rz_net_buy,
                    rq_balance, rq_remain, rq_sell, rq_redeem, rq_net_sell)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
                try:
                    cur.execute(sql, row)
                except sqlite3.IntegrityError:
                    has_dup = True
                    break
        
            print('done', symbol, name, page_num)
            time.sleep(2)
            if has_dup:
                break

        conn.commit()
        cur.close()
        conn.close()

        print('done', symbol, name)
        with open(dup_file, 'a') as fp:
            fp.write(dup + '\n')

if __name__ == '__main__':
    save()
