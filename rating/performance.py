#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- author: Sean -*-

import numpy as np
import pandas as pd
import datetime


input_file = "raw.csv"
print "loading data from %s" % input_file
data = pd.read_csv(input_file,names = ["id","name","desc","trade_date","trade_value","accu_value"], header=0,dtype = {'id':int,'trade_value':float,'accu_value':float},parse_dates = ["trade_date"])

data = data.sort(["id","trade_date"])
data["trade_biweek"] = [ x.year * 100 + int(datetime.datetime.strftime(x,"%U"))/2 for x in data.trade_date ]

data_grouped = data.groupby(["id","trade_biweek"])
data['loss'] = data_grouped.accu_value.apply(lambda x: pd.expanding_apply(x,lambda y: (y[-1]/(np.max(y)))-1))
data['biggest_loss'] = data.loc[data_grouped.loss.idxmin(),'loss']
data['biggest_loss_day'] = data.loc[data_grouped.loss.idxmin(),'trade_date']


data_result = pd.DataFrame()

data_result['biweek_start_value'] = data_grouped.accu_value.first()

data_result['biweek_last_value'] = data_grouped.accu_value.last()
data_result['earning1'] = (data_result.biweek_last_value / data_result.biweek_start_value ) - 1
data_result['earning2'] = pd.concat([ pd.rolling_apply(v.biweek_last_value,2,lambda x:(x[1]/x[0])-1) for k,v in data_result.reset_index(level=0).groupby(["id"])]).values

#data_result['gain2'] = pd.rolling_apply(data_result['last'],2,lambda x:(x[1]/x[0])-1)

data_result['earning'] = np.where(pd.isnull(data_result['earning2']), data_result['earning1'], data_result['earning2'])

data['rtn'] = data.groupby(['id']).apply(lambda y:pd.rolling_apply(y['accu_value'],2,lambda x:(x[1]/x[0])-1)).values


data_result['rtn_std'] = data_grouped.rtn.std()

data_result['win_days'] = data_grouped.rtn.apply(lambda x: len(x[x>=0]))
data_result['lose_days'] = data_grouped.rtn.apply(lambda x: len(x[x<0]))

data_result['win_rate'] = data_result['win_days'] / (data_result['win_days'] + data_result['lose_days'])

data_result['biggest_loss_day'] = data.set_index(["id","trade_biweek"]).biggest_loss_day.dropna()
data_result['biggest_loss'] = data.set_index(["id","trade_biweek"]).biggest_loss.dropna()

data_result['deltadays_with_hs300'] = data_result.biggest_loss_day - data_result.loc[300].biggest_loss_day

data_result.to_csv("result.csv",encoding='utf-8')