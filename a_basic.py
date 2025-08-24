#!/usr/bin/env python
#encoding:utf-8
#A股基础信息

import os
import json
import requests
import a_all

def get_data(symbol):
    url = 'https://stock.xueqiu.com/v5/stock/f10/cn/company.json?symbol=' + symbol.upper()
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6',
        'cache-control': 'no-cache',
        'origin': 'https://xueqiu.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://xueqiu.com/snowman/S/SH601127/detail',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    }
    cookies = {
        'xq_a_token': 'f03e970c98eae52ec63285d1bf12945d386610c4',
        'xqat': 'f03e970c98eae52ec63285d1bf12945d386610c4',
        'xq_r_token': 'cb9cd21c6bd16d78a9d44248d963861807a7c590',
        'xq_id_token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTc1NzcyNjA1MCwiY3RtIjoxNzU1OTk5NTE1MzkyLCJjaWQiOiJkOWQwbjRBWnVwIn0.oycPeCl2n4Zv-cGVQOFgZ-UcMxR7WDXc80KF0tw54IvDT2X238iJw2EPVwlCTUBpCR5oag-owlRzn92nmHoPooenpIvxsQYBGhFqTmmgF71wXKRYYiMI5J43FPxTS5xk6swuU_nQ1NAdDMqnkrK6Gb5YjgfMkNLHks1jpQm6wm58zuWd0wA6usaL2IaXcckGepbX3ImESkdFLSKeeZ3QQlq53i6X4t5BsSaZYtrpALOuyr7Vhi7VihrLos3NPSVPFt3y0u5q1Bz47yQ_WPdRbgcqa4SX6yr-mr8CY_GAeVF354454H2n_Xo4u0lFYTIuyA0qAXn2HbFEGelrUuUDuQ',
        'cookiesu': '311755999548731',
        'u': '311755999548731',
        'device_id': '85373b0d9e8237c2139ac8a36a87593d',
        'Hm_lvt_1db88642e346389874251b5a1eded6e3': '1755999549',
        'HMACCOUNT' : 'F05CFA5EE4D2AF59',
        'Hm_lpvt_1db88642e346389874251b5a1eded6e3': '1755999861',
        'ssxmod_itna': '1-Qq_xcD0Qitq4yQDhrFDCDm6D_oyIpPDXDUqAj=Gg0DFqAPDODCOiID2P7QQr_5qihjD0Keqxiwxr0SxGXq_Qx0=HDf40W_C1n4_zqsG00x4NhZ7xhzwhclbkR6NtBruxF8_BgBwU6Dv=ytSApexGImY5Dbb3DAueD7L5XDYYDC4GwDGoD34DiDDP3xDUrhneD72udylTq=EroGnTDm83QQxDgnPD1F6nTZni4leDARPGyW63EbOWTRxGnD0jA=xqNx031To8mp4GW84TmrKGu4I2CmoWb=ce0P2xMlRd7jieqzfDdFBYPiR8Q0iU5QfGbG2GtRqz0GYG8WwH_AY3IYfvPP2C74gtHSmRSex/CrOnTouT/Ur7G0=SrN/riUirbwH44bzDQQDTjy4eD',
        'ssxmod_itna2': '1-Qq_xcD0Qitq4yQDhrFDCDm6D_oyIpPDXDUqAj=Gg0DFqAPDODCOiID2P7QQr_5qihjD0Keqxiwxr0exD3rYexaSnAronMH0dPHCL37e4D',
    }
    resp = requests.get(url, headers=headers, cookies=cookies)
    return json.loads(resp.text)

def save():
    all_symbol = a_all.read()
    for symbol, name in all_symbol.items():
        symbol_dir = 'data/' + symbol
        if not os.path.exists(symbol_dir):
            os.mkdir(symbol_dir)

        data = get_data(symbol)
        print(data['data'])
        with open(symbol_dir + '/basic.json', 'w+', encoding='utf-8') as fp:
            fp.write(json.dumps(data['data'], ensure_ascii=False))

def read(symbol):
    with open('data/' + symbol + '/basic.json', 'r', encoding='utf-8') as fp:
        return json.loads(fp.read())

if __name__ == '__main__':
    save()
