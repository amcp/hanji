#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import time
import re
import fileinput
import sys
import sre_compile


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


base_re = re.compile(u".*(平成|昭和)(\d{1,2}|元)年(\d{1,2})月(\d{1,2})日.*", 128)


def convert_jp_date_to_utc(date_string):
    date_matches = base_re.match(date_string)
    if not date_matches:
        eprint('regex broken for ' + date_string)
        return -1
    nengo = date_matches.group(1)
    year = date_matches.group(2)
    month = date_matches.group(3)
    day = date_matches.group(4)

    if u'元' == year:
        year = 1

    if '平成' == nengo:
        base_year = 1988
    elif '昭和' == nengo:
        base_year = 1925
    actual_year = base_year + year
    str = str(actual_year) + '/' + str(month) + '/' + str(day)

    ts = time.strptime(str, "%y/%m/%d")
    print(time.strftime(ts))


#TODO test and fix
convert_jp_date_to_utc("昭和60年3月9日")

# if __name__ == '__main__':
#     for line in fileinput.input():
#         result = convert_jp_date_to_utc(line.strip())
#         if result != 0:
#             sys.exit(result)
#     sys.exit(0)
