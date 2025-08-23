#!/usr/bin/env python
#encoding:utf-8
#下载A股全量股票代码

import json
import requests

def get_page_data(offset, count=20):
    url = 'https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList?_appver=11.17.0&board_code=aStock&sort_type=price&direct=down&offset=%d&count=%d' % (offset, count)
    resp = requests.get(url)
    return json.loads(resp.text)

def get_all_data(count=200):
    offset = 0
    all_data = []
    while True:
        data = get_page_data(offset, count)
        rank_list = data['data']['rank_list']
        all_data.extend(rank_list)

        if len(all_data) == data['data']['total']:
            break

        offset += count
    return all_data

def read():
    exists = {}
    with open('data/a_all.csv', 'r', encoding='utf-8') as fp:
        lines = fp.readlines()
        for line in lines:
            code, name = line.strip().split(' ')
            exists[code] = name
    return exists

def save():
    all_data = get_all_data()
    with open('data/a_all.csv', 'w+', encoding='utf-8') as fp:
        for d in all_data:
            fp.write('%s %s\n' % (d['code'], d['name'].replace(' ', '', len(d['name']))))

if __name__ == '__main__':
    save()
    print(read())
