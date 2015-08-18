#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__: xuxun22@gmail.com

# 1. get all products by issue a search:
# http://mall.simuwang.com/ajax_api_ltotal.php?action=mall_filter&page_index=1&page_size=100&condition=sort_name:ret_incep;sort_asc:DESC;

# 2. then get historical net value for each product
# http://dc.simuwang.com/charts_for_mall/index_highcharts.php?fund_id=HF000000G2&type=jzdb_stock

import pandas as pd
import numpy as np
import requests
import os
from random import random
from time import sleep

def _request(url):
    try:
        wait = int(random() * 3)
        #sleep(wait) # add a random wait to reduce the risk of anti-crawler
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
        return {}
    except:
        return {}

def get_historical_data(fund_id):
    print ">>> Getting historical data for %s" % fund_id
    base_url = "http://dc.simuwang.com/charts_for_mall/index_highcharts.php?type=jzdb_stock&fund_id="
    url = base_url + fund_id
    data = _request(url).get('data')
    try:
        net_values = pd.DataFrame(data["0"],columns = ("microseconds","value"))
        net_values["date"] = pd.to_datetime((net_values["microseconds"] / 1000),unit='s')
        net_values["id"] = fund_id
        net_values.drop(("microseconds"),axis=1,inplace =True)
        return net_values
    except:
        print ">>> WARN: %s missing" % fund_id
        return None


def get_all_products():
    print ">>> Getting all products now ..."
    total_pages = 1
    cur_page = 1
    funds = pd.DataFrame()
    base_url = "http://mall.simuwang.com/ajax_api_ltotal.php?action=mall_filter&page_size=100&condition=sort_name:ret_incep;sort_asc:DESC&page_index="
    while (cur_page <= total_pages):
        print ">>> Trying %d page out of %s total pages" % (cur_page, total_pages)
        url = base_url + str(cur_page)
        data = _request(url)
        total_pages = data.get('pager')['total_pages']
        funds = funds.append(pd.DataFrame(data.get('data')))
        cur_page = cur_page + 1

    funds.drop_duplicates(["fund_id"],inplace = True)
    return funds

def crawl():
    output_dir = "data"
    funds = get_all_products()
    funds.to_csv("funds.csv",index=False,encoding="utf-8")
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    historical_data = pd.DataFrame()
    for fund_id in funds["fund_id"]:
        target_name = os.path.join(output_dir,(fund_id + ".csv"))
        temp_target = target_name + ".tmp"
        if not os.path.exists(target_name):
            fund_data = get_historical_data(fund_id)
            if fund_data is not None:
                fund_data.to_csv(temp_target,index=False,encoding="utf-8")
                os.rename(temp_target,target_name)
                historical_data = historical_data.append(fund_data)
        else:
            fund_data = pd.read_csv(target_name, encoding="utf-8")
            historical_data = historical_data.append(fund_data)

    historical_data.to_csv("all_data.csv",index=False,encoding="utf-8")

if __name__ == "__main__":
    crawl()
