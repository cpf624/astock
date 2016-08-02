#!/usr/bin/env python
#encoding:utf-8
# Author:   jhat
# Date:     2016-01-28
# Email:    cpf624@126.com
# Home:     http://jhat.pw
# Vim:      tabstop=4 shiftwidth=4 softtabstop=4

import os

import MySQLdb

def __reverse_readline__(filename, buf_size=8192):
    """a generator that returns the lines of a file in reverse order"""
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first 
                if buffer[-1] is not '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if len(lines[index]):
                    yield lines[index]
        # Don't yield None if the file was empty
        if segment is not None:
            yield segment

def save_history(number, date, directory='.'):
    '''save stock history deal data

    :param number:      stock number
    :param data:        stock deal date(YYYY-MM-DD)
    :param directory:   directory of the data file
    '''

    table_name = 'hd_' + number + '_' + date.split('-')[0]
    db = MySQLdb.connect(host="127.0.0.1", user="stock", passwd="stock", db="stock")
    cur = db.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS %s (
        time        TIMESTAMP NOT NULL,
        price       INT NOT NULL,
        changes     INT NOT NULL,
        volume      BIGINT NOT NULL,
        amounts     BIGINT NOT NULL,
        status      TINYINT NOT NULL
        ) ENGINE = MyISAM;''' % (table_name,))

    try:
        for line in __reverse_readline__(directory + '/' + number + '/' + date):
            time, price, changes, volume, amounts, status = line.split('\t')
            if time == '成交时间':
                break
            price = int(float(price) * 100)
            if changes == '--':
                changes = 0
            else:
                changes = int(float(changes) * 100)
            volume = int(volume)
            amounts = int(float(amounts) * 100)
            status = {"买盘": 10, "卖盘": 30, "中性盘": 20}.get(status, "0")
            cur.execute('''INSERT INTO %s(time, price, changes, volume, amounts, status)
                    VALUES ("%s %s", "%d", "%d", "%d", "%d", "%d")''' % (table_name,
                        date, time, price, changes, volume, amounts, status))
    except Exception as e:
        cur.execute('DROP TABLE IF EXISTS ' + table_name)
    finally:
        db.close()
