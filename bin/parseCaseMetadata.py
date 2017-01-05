#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import fileinput
import sys
import time
import re
from collections import OrderedDict
import urllib
import requests
from bs4 import BeautifulSoup
import boto3


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


SEARCH_CONTENT = u"search_content"
SAIKOSAI_HANREI = u"最高裁判例"
KOSAI_HANREI = u"高裁判例"
KAKYUSAI_HANREI = u"下級裁判例"
GYOSEIJIKEN_SAIBANREI = u"行政事件裁判例"
RODOJIKEN_SAIBANREI = u"労働事件裁判例"
CHITEKIZAISAN_SAIBANREI = u"知的財産裁判例"


category_number_map = OrderedDict()
category_number_map[SAIKOSAI_HANREI] = 2
category_number_map[KOSAI_HANREI] = 3
category_number_map[KAKYUSAI_HANREI] = 4
category_number_map[GYOSEIJIKEN_SAIBANREI] = 5
category_number_map[RODOJIKEN_SAIBANREI] = 6
category_number_map[CHITEKIZAISAN_SAIBANREI] = 7


SAIBAN_SHUBETSU = u"裁判種別"
BUNYA = u"分野"
KENRI_SHUBETSU = u"権利種別"
GENSHIN_SAIBANSHOMEI = u"原審裁判所名"
SAIBANSHOMEI = u"裁判所名"
SAIBANSHOMEI_BU = u"裁判所名・部"
JIKEN_BANGO = u"事件番号"
SAIBAN_NENGAPPI = u"裁判年月日"
JIKENMEI = u"事件名"
HANJI_JIKO = u"判示事項"
SAIBAN_YOSHI = u"裁判要旨"
HANJI_JIKO_YOSHI = u"判示事項の要旨"
KEKKA = u"結果"
GENSHIN_KEKKA = u"原審結果"
GENSHIN_JIKEN_BANGO = u"原審事件番号"
HOTEIMEI = u"法廷名"
HANREISHU = u"判例集等巻・号・頁"
KOSAI_HANREISHU = u"高裁判例集登載巻・号・頁"
SOSHO_RUIKEI = u"訴訟類型"


def sanitize(raw):
    return " ".join(raw.strip().replace("\n", " ").split())


def get_only_matching_div_string(tag, literal):
    tags = tag.find_all("div", string=re.compile("" + literal))
    tagdict = OrderedDict()
    for tag in tags:
        tagdict[sanitize(tag.string)] = tag
    shortest_key = min(tagdict.keys(), key=len)
    div = tagdict[shortest_key].find_next_sibling("div")
    return None if div is None else sanitize(div.string)


def get_only_matching_div_text(tag, literal):
    for elem in tag.find_all("div", string=re.compile(literal)):
        div = elem.find_next_sibling("div")
        return None if div is None else div.text.strip()
    return None


def convert_jp_date_to_utc(date_string):
    step1 = re.sub(r'(昭和\|平成\|年\|月\|日)', '@', date_string)
    print(step1)
    ts = time.strptime(str, "%y/%m/%d")
    return time.strftime(ts)


def get_case_url(category, courts_id):
    return "http://www.courts.go.jp/app/hanrei_jp/detail" + str(category) + "?id=" + str(courts_id)


def add_non_empty_string_attribute(dict, key, value, required=False):
    if value:
        dict[key] = value
    elif required:
        raise ValueError("required attribute value was empty")


def print_case_metadata(courts_id):
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    table = dynamodb.Table('case_data')

    # first, assume everything is supreme court first to get some basic metadata.
    url = get_case_url(category_number_map[SAIKOSAI_HANREI], courts_id)
    r = requests.get(url)
    if r.status_code == 404:
        return 0 #do nothing for a case that does not exist
    elif r.status_code != 200:
        eprint(r)
        return -1
    soup = BeautifulSoup(r.text, "html.parser")
    temp_content = soup.find(id=SEARCH_CONTENT)

    case_number = get_only_matching_div_string(temp_content, JIKEN_BANGO)
    case_number_re = re.compile(u"(昭和|平成)([0-9]{1,4})(\(\D*\)\D*)([0-9]{1,5})", re.UNICODE)
    case_number_match_obj = case_number_re.match(case_number)
    filters = OrderedDict()
    if case_number_match_obj:
        filters[u"filter[jikenGengo]"] = case_number_match_obj.group(1).encode("utf-8")
        filters[u"filter[jikenYear]"] = case_number_match_obj.group(2)
        filters[u"filter[jikenCode]"] = case_number_match_obj.group(3).encode("utf-8")
        filters[u"filter[jikenNumber]"] = case_number_match_obj.group(4)

    ruling_date = get_only_matching_div_string(temp_content, SAIBAN_NENGAPPI)
    ruling_date_re = re.compile(u"(昭和|平成)([0-9]{1,4}|元)年([0-9]{1,2})月([0-9]{1,2})日", re.UNICODE)
    ruling_date_match_obj = ruling_date_re.match(ruling_date)
    if ruling_date_match_obj:
        if ruling_date_match_obj.group(2) == u'元':
            year = u'1'
        else:
            year = ruling_date_match_obj.group(2)
        filters[u"filter[judgeDateMode]"] = 1  # exact date
        filters[u"filter[judgeGengoFrom]"] = ruling_date_match_obj.group(1).encode("utf-8")
        filters[u"filter[judgeYearFrom]"] = year
        filters[u"filter[judgeMonthFrom]"] = ruling_date_match_obj.group(3)
        filters[u"filter[judgeDayFrom]"] = ruling_date_match_obj.group(4)
    else:
        eprint("unable to match ruling dates for hanji_id=" + courts_id)
        return -1

    # second, get the search overview page to see which categories populate
    search_url = u"http://www.courts.go.jp/app/hanrei_jp/list1?action_search=検索&" + urllib.urlencode(filters)
    r = requests.get(search_url)
    if r.status_code == 404:
        return 0  # do nothing for a case that does not exist
    elif r.status_code != 200:
        eprint(r)
        return -1

    soup = BeautifulSoup(r.text, "html.parser")
    waku_list = soup.select(".waku")
    if len(waku_list) != 1:
        eprint("there was not exactly one waku but " + str(len(waku_list)) + " waku elements in the results for id=" + courts_id)
        return -1
    waku = waku_list[0]
    link_table_cell_list = waku.select(".width_hs_left")
    if len(link_table_cell_list) < 1:
        eprint("there was not at least one width_hs_left but zero width_hs_left elements in the results for id=" + courts_id)
        return -1
    row = 0
    courts_id_stripped = courts_id.lstrip("0")
    if len(link_table_cell_list) > 1:
        row = -1
        for j in range(0, len(link_table_cell_list)):
            if link_table_cell_list[j].find_all(href = lambda e: e and re.compile(courts_id_stripped).search(e)):
                row = j
                break
        if row == -1:
            eprint("there were multiple link tables but none pointed to " + courts_id)
            return -1
    links = link_table_cell_list[row].find_all("a")
    categories = map(lambda x: x.string.strip(), links)

    if len(categories) < 1:
        eprint("there were no categories for id=" + courts_id)
        return -1

    for category in categories:
        url = get_case_url(category_number_map[category], courts_id)
        r = requests.get(url)
        if r.status_code == 404:
            return 0 # do nothing for a case that does not exist
        elif r.status_code != 200:
            eprint(r)
            return -1
        soup = BeautifulSoup(r.text, "html.parser")
        item = OrderedDict()
        content = soup.find(id=SEARCH_CONTENT)
        #   common properties
        add_non_empty_string_attribute(item, "hanji_id", courts_id)
        add_non_empty_string_attribute(item, "category", category, True)
        add_non_empty_string_attribute(item, "detail_url", url)
        add_non_empty_string_attribute(item, "case_number", get_only_matching_div_string(content, JIKEN_BANGO))
        add_non_empty_string_attribute(item, "case_name", get_only_matching_div_string(content, JIKENMEI))
        add_non_empty_string_attribute(item, "ruling_date", get_only_matching_div_string(content, SAIBAN_NENGAPPI))
        add_non_empty_string_attribute(item, "caseUrl", "http://www.courts.go.jp" + content.find_all("a", string=re.compile(u"全文"))[0]['href'])
        if category == SAIKOSAI_HANREI:
            add_non_empty_string_attribute(item, "case_claims", get_only_matching_div_text(content, HANJI_JIKO))
            add_non_empty_string_attribute(item, "ruling_summary", get_only_matching_div_text(content, SAIBAN_YOSHI))
            add_non_empty_string_attribute(item, "ruling", get_only_matching_div_string(content, KEKKA))
            add_non_empty_string_attribute(item, "parent_jurisdiction", get_only_matching_div_string(content, GENSHIN_SAIBANSHOMEI))
            add_non_empty_string_attribute(item, "parent_case_number", get_only_matching_div_string(content, GENSHIN_JIKEN_BANGO))
            add_non_empty_string_attribute(item, "supreme_ruling_collection_volume_page", get_only_matching_div_string(content, HANREISHU))
            add_non_empty_string_attribute(item, "courtroom", get_only_matching_div_string(content, HOTEIMEI))
            add_non_empty_string_attribute(item, "ruling_type", get_only_matching_div_string(content, SAIBAN_SHUBETSU))
            add_non_empty_string_attribute(item, "determining_laws", get_only_matching_div_string(content, u"参照法条"))
        elif category == KOSAI_HANREI:
            add_non_empty_string_attribute(item, "case_claims", get_only_matching_div_text(content, HANJI_JIKO))
            add_non_empty_string_attribute(item, "ruling_summary", get_only_matching_div_text(content, SAIBAN_YOSHI))
            add_non_empty_string_attribute(item, "courthouse_section", get_only_matching_div_string(content, SAIBANSHOMEI_BU))
            add_non_empty_string_attribute(item, "ruling", get_only_matching_div_string(content, KEKKA))
            add_non_empty_string_attribute(item, "parent_jurisdiction", get_only_matching_div_string(content, GENSHIN_SAIBANSHOMEI))
            add_non_empty_string_attribute(item, "parent_case_number", get_only_matching_div_string(content, GENSHIN_JIKEN_BANGO))
            add_non_empty_string_attribute(item, "high_ruling_collection_volume_page", get_only_matching_div_string(content, KOSAI_HANREISHU))
        elif category == KAKYUSAI_HANREI:
            add_non_empty_string_attribute(item, "courthouse_section", get_only_matching_div_string(content, SAIBANSHOMEI_BU))
            add_non_empty_string_attribute(item, "ruling", get_only_matching_div_string(content, KEKKA))
            add_non_empty_string_attribute(item, "parent_jurisdiction", get_only_matching_div_string(content, GENSHIN_SAIBANSHOMEI))
            add_non_empty_string_attribute(item, "parent_case_number", get_only_matching_div_string(content, GENSHIN_JIKEN_BANGO))
            add_non_empty_string_attribute(item, "parent_ruling", get_only_matching_div_string(content, GENSHIN_KEKKA))
            add_non_empty_string_attribute(item, "case_claim_summary", get_only_matching_div_text(content, HANJI_JIKO_YOSHI))
        elif category == GYOSEIJIKEN_SAIBANREI:
            add_non_empty_string_attribute(item, "courthouse", get_only_matching_div_string(content, SAIBANSHOMEI))
            add_non_empty_string_attribute(item, "case_type", get_only_matching_div_string(content, BUNYA))
            add_non_empty_string_attribute(item, "case_claims", get_only_matching_div_text(content, HANJI_JIKO))
            add_non_empty_string_attribute(item, "ruling_summary", get_only_matching_div_text(content, SAIBAN_YOSHI))
        elif category == RODOJIKEN_SAIBANREI:
            add_non_empty_string_attribute(item, "courthouse", get_only_matching_div_string(content, SAIBANSHOMEI))
            add_non_empty_string_attribute(item, "case_type", get_only_matching_div_string(content, BUNYA))
        elif category == CHITEKIZAISAN_SAIBANREI:
            add_non_empty_string_attribute(item, "courthouse", get_only_matching_div_string(content, SAIBANSHOMEI))
            add_non_empty_string_attribute(item, "case_rights", get_only_matching_div_string(content, KENRI_SHUBETSU))
            add_non_empty_string_attribute(item, "case_paradigm", get_only_matching_div_string(content, SOSHO_RUIKEI))
        table.put_item(Item=item)
        # print(repr(table.get_item(Key = {'hanji_id': item["hanji_id"], 'category': item["category"]})['Item']).decode("unicode-escape"))
    print(courts_id[-3:] + '/' + courts_id)
    return 0


if __name__ == '__main__':
    for line in fileinput.input():
        result = print_case_metadata(line.rstrip())
        if result != 0:
            sys.exit(result)
    sys.exit(0)
