#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- author: Sean -*-

import numpy as np
import pandas as pd
import datetime
import httplib
from StringIO import StringIO
import json
from itertools import izip_longest,izip

input_file = "data20150702.txt"

index_symbols =[ { "key":-300, "symbol":"000300.SS","ratio":0.35 },{"key": -923,"symbol":"000923.SS","ratio":0.15} ,{"key":-905,"symbol":"000905.SS","ratio":0.15},{"key":-13, "symbol":"000013.SS","ratio":0.35} ]
benchmark_id = -300
enterprise_id = -923
GG_id = 0
GG_index_id = 0

api_host = 'api.wmcloud.com'
api_token = '261819a18278577a2d7f9c6341f7c6cd8708c5a20e7685f406ba9e5e44095645'

class ApiException(Exception):
    def __init__(self,value):
        self.value = value

    def __str__(self):
        return repr(self.value)

def api_request(url,retJson=False):
    httpClient = httplib.HTTPSConnection(api_host)
    
    httpClient.request('GET', '/data' + url, headers = {"Authorization": "Bearer " + api_token })
    response = httpClient.getresponse()
    msg = response.read()
    if response.status == 200:
        return msg
    else:
        raise ApiException(msg)

print ">>>loading data from %s" % input_file
data = pd.read_csv(input_file,names = ["id","name","desc","trade_date","trade_value","accu_value"], header=0,dtype = {'id':int,'trade_value':float,'accu_value':float},parse_dates = ["trade_date"])
data = data.fillna({'accu_value':data.trade_value})

#start_date = datetime.date(2014,10,8)
#end_date = datetime.date(2015,3,28)
start_date = data.trade_date.min()
end_date = data.trade_date.max()


GG_index = pd.DataFrame({"id":GG_index_id,"name":"GG_index","desc":"GG_index","trade_value":0,"trade_date":data.trade_date}).drop_duplicates().sort(["trade_date"]).set_index(["trade_date"])
indexes = []

for index_symbol in index_symbols:
	print ">>>Fetching index data of %s" % index_symbol['symbol']
#	index_data = pd.read_csv("http://ichart.yahoo.com/table.csv?s=%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s" % (index_symbol['symbol'],start_date.month - 1, start_date.day, start_date.year,end_date.month - 1, end_date.day, end_date.year),parse_dates = ["Date"])
#	index_data = index_data.sort(["Date"]).reset_index(drop=True)
#
#	formated_index = pd.DataFrame(columns = ["id","name","desc","trade_date","trade_value","accu_value"])
#
#	formated_index["trade_date"] = index_data["Date"]
#	formated_index["trade_value"] = index_data["Close"]
#	formated_index["accu_value"] = index_data["Close"]
#
#	formated_index["id"] = index_symbol['key']
#	formated_index["name"] = index_symbol['symbol']
#	formated_index["desc"] = index_symbol['symbol']
#	data = data.append(formated_index)
#

	index_data = pd.read_csv(StringIO(api_request("/api/market/getMktIdxd.csv?ticker=%s&beginDate=%s&endDate=%s&field=ticker,tradeDate,closeIndex" % (index_symbol['symbol'].split(".")[0], start_date.strftime("%Y%m%d"),end_date.strftime("%Y%m%d")))),parse_dates = ["tradeDate"])
	index_data = index_data.sort(["tradeDate"]).reset_index(drop=True)

	formated_index = pd.DataFrame(columns = ["id","name","desc","trade_date","trade_value","accu_value"])

	formated_index["trade_date"] = index_data["tradeDate"]
	formated_index["trade_value"] = index_data["closeIndex"]
	formated_index["accu_value"] = index_data["closeIndex"]

	formated_index["id"] = index_symbol['key']
	formated_index["name"] = index_symbol['symbol']
	formated_index["desc"] = index_symbol['symbol']
	indexes.append(index_symbol["key"])

	GG_index["trade_value"] = GG_index.trade_value + formated_index.loc[:,["trade_date","trade_value"]].set_index(["trade_date"]) * index_symbol["ratio"]

	data = data.append(formated_index)


GG_index["accu_value"] = GG_index["trade_value"]
data = data.append(GG_index.reset_index())

data = data[data.trade_date.isin(data[data.id == benchmark_id].trade_date)]
data = data.drop_duplicates(subset=["id","trade_date"])
data = data.sort(["id","trade_date"]).reset_index(drop=True)
#data = data[data.trade_date.isin(np.arange(start_date, end_date , dtype='datetime64[D]'))]
#data = data[data.trade_date.isin(np.arange(start_date, end_date ))]
data["trade_biweek"] = [ x.year * 100 + int(datetime.datetime.strftime(x,"%U"))/2 for x in data.trade_date ]
data_grouped = data.groupby(["id","trade_biweek"])
data['loss'] = data_grouped.accu_value.apply(lambda x: pd.expanding_apply(x,lambda y: (y[-1]/(np.max(y)))-1))
data['biggest_loss'] = data.loc[data_grouped.loss.idxmin(),'loss']
data['biggest_loss_day'] = data.loc[data_grouped.loss.idxmin(),'trade_date']

data_result = pd.DataFrame()
data_result['biweek_first_date'] = data_grouped.trade_date.first()
data_result['biweek_last_date'] = data_grouped.trade_date.last()
data_result['biweek_start_value'] = data_grouped.accu_value.first()

data_result['biweek_last_value'] = data_grouped.accu_value.last()
data_result['earning1'] = (data_result.biweek_last_value / data_result.biweek_start_value ) - 1
data_result['earning2'] = pd.concat([ pd.rolling_apply(v.biweek_last_value,2,lambda x:(x[1]/x[0])-1) for k,v in data_result.reset_index(level=0).groupby(["id"])]).values

data_result['earning'] = np.where(pd.isnull(data_result['earning2']), data_result['earning1'], data_result['earning2'])

data['rtn'] = data.groupby(['id']).apply(lambda y:pd.rolling_apply(y['accu_value'],2,lambda x:(x[1]/x[0])-1)).values

#it's a hacking,needa fix
#data['rtn'] = data.rtn.fillna(0)

data_result['volatility'] = data_grouped.rtn.std()
data_result['win_days'] = data[data.rtn>= 0].groupby(["id","trade_biweek"]).rtn.count()
data_result['lose_days'] = data[data.rtn< 0].groupby(["id","trade_biweek"]).rtn.count()

data_result['biggest_loss_day'] = data.set_index(["id","trade_biweek"]).biggest_loss_day.dropna()
data_result['biggest_loss'] = data.set_index(["id","trade_biweek"]).biggest_loss.dropna()

data_result['deltadays_with_benchmark'] = [x.days for x in abs(data_result.biggest_loss_day - data_result.loc[benchmark_id].biggest_loss_day)]

data_result = data_result.reset_index()
data_result = data_result.fillna({"win_days":0,"lose_days":0})
data_result['win_ratio'] = data_result['win_days'] / (data_result['win_days'] + data_result['lose_days'])
data_result = data_result.drop(labels=["earning1","earning2","biweek_start_value","biweek_last_value"],axis = 1)

#samples = pd.read_csv("samples.txt",header=None,names = ["id"])
#sampled_data = data_result[data_result.id.isin(samples.id)]
sampled_data = data_result[~data_result.id.isin(indexes)]
#sampled_data = data_result
sampled_data = sampled_data.reset_index(drop=True)

sampled_data["biweek_days"] = [x.days + 1 for x in  (sampled_data.biweek_last_date - sampled_data.biweek_first_date)]
sampled_data["annual_earning"] = sampled_data.earning * 360 / sampled_data.biweek_days
sampled_data["annual_volatility"] = sampled_data.volatility * np.sqrt(360)
sampled_data["earning_loss_ratio"] = np.where( (sampled_data.earning > 0) & (sampled_data.biggest_loss != 0), sampled_data.earning / np.abs(sampled_data.biggest_loss),np.NaN)

metrics = ["annual_earning","annual_volatility","win_ratio","deltadays_with_benchmark","earning_loss_ratio"]
sampled_metrics = sampled_data.loc[:,["trade_biweek"] + metrics]

mean_metrics = sampled_metrics.groupby("trade_biweek").mean()
std_metrics = sampled_metrics.groupby("trade_biweek").std()
earning_loss_ratio_min = pd.DataFrame()
earning_loss_ratio_min['earning_loss_ratio_min'] = sampled_data.groupby(["trade_biweek"]).earning_loss_ratio.min()
sampled_data = sampled_data.join(mean_metrics,on='trade_biweek',rsuffix = "_mean")
sampled_data = sampled_data.join(std_metrics,on='trade_biweek',rsuffix = "_std")
sampled_data = sampled_data.join(earning_loss_ratio_min,on='trade_biweek')

for m in metrics:
    sampled_data[m + "_distance"] = ((sampled_data[m] - sampled_data[m + "_mean"])/sampled_data[m + "_std"])
    sampled_data[m + "_distance"] = np.minimum(3.0,np.maximum(-3.0,sampled_data[m + "_distance"]))

sampled_data = sampled_data.fillna({"earning_loss_ratio_distance": (sampled_data["earning_loss_ratio_min"] - sampled_data["earning_loss_ratio_mean"])/sampled_data["earning_loss_ratio_std"] })

sampled_data["performance1"] = sampled_data["annual_earning_distance"] * 0.1 + 0.1 * sampled_data["win_ratio_distance"] - 0.42 * sampled_data["annual_volatility_distance"] + 0.18 * sampled_data["deltadays_with_benchmark_distance"] + 0.2 * sampled_data["earning_loss_ratio_distance"]

sampled_data["trade_year"] = [int(x/100) for x in sampled_data.trade_biweek]
sampled_data_by_year = pd.DataFrame()
sampled_data_by_year["performance1"] = sampled_data.groupby(["id","trade_year"]).performance1.mean()

sampled_data.to_csv("sampled_data.csv",encoding='utf-8',index=False)

performance = pd.DataFrame()
#performance["hf"] = sampled_data_by_year.reset_index().groupby(["id"]).performance1.mean()
performance["hf"] = sampled_data.groupby(["id"]).performance1.mean()
#hacking for GG
#first_biweek = np.sort(data.trade_biweek)[0]
#performance["hf"] = sampled_data[sampled_data.trade_biweek != first_biweek].groupby(["id"]).performance1.mean()

performance["earning"] = data.groupby(["id"]).accu_value.last() / data.groupby(["id"]).accu_value.first()  -1
performance['volatility'] = data.groupby(["id"]).rtn.std()

data["overall_loss"] = data.groupby(["id"]).accu_value.apply(lambda x: pd.expanding_apply(x,lambda y: (y[-1]/(np.max(y)))-1))

performance["loss"] = data.groupby(["id"]).overall_loss.min()
performance["sharp"] = -(performance.earning - 4 * abs(performance.loss)) / (performance.volatility * np.sqrt(360))

performance['enterprise_earning'] = (data.groupby(["id"]).accu_value.last() / data.groupby(["id"]).accu_value.first()  -1).loc[enterprise_id]
performance["period"] = (data.groupby(["id"]).trade_date.last() - data.groupby(["id"]).trade_date.first())
performance["implied_volatility"] = (((performance.earning - performance.enterprise_earning) * 360 /[(x.days + 1) for x in performance.period])/2)/ (np.sqrt(360)* performance.volatility)

performance['win_days'] = data[data.rtn>= 0].groupby(["id"]).rtn.count()
performance['lose_days'] = data[data.rtn< 0].groupby(["id"]).rtn.count()

performance = performance.fillna({"win_days":0,"lose_days":0})
performance['win_ratio'] = performance['win_days'] / (performance['win_days'] + performance['lose_days'])


data_result["biggest_loss_rank"] = data_result.groupby(["id"]).biggest_loss.rank('min')
data_result["earning_rank"] = data_result.groupby(["id"]).earning.rank('min',ascending=False)

for m,v in {"sharp":1,"implied_volatility":2}.items():
	performance[m + "_rank"] = performance[m].rank('min',ascending=True,pct = True)
	performance[m+"_adj"] = np.where(performance[m] > v,np.where(performance[m+"_rank"] <0.85, 0,
        np.where(((performance[m+"_rank"] >=0.85) & (performance[m+"_rank"] <0.9)), -0.05,
        np.where(((performance[m+"_rank"] >=0.9) & (performance[m+"_rank"] <0.95)), -0.10,
        np.where(((performance[m+"_rank"] >=0.95) & (performance[m+"_rank"] <0.99)), -0.15,
        np.where((performance[m+"_rank"] >=0.99), -0.25, 0))))),0)

performance.to_csv("performance.csv",encoding='utf-8',index=True)
performance[(performance.win_ratio < 0.8) & (performance.volatility >0.00075)].to_csv("performance1.csv",encoding='utf-8',index=True)
performance[(performance.win_ratio >= 0.8) | (performance.volatility <= 0.00075)].to_csv("performance2.csv",encoding='utf-8',index=True)

#exit(1)

for index in [-300,-905]:
	for m in ["biggest_loss_rank","earning_rank"]:
		a = data_result[data_result[m] <= 3].sort([m]).loc[data_result.id == index,"trade_biweek"].values
		performance[m + str(index)] = sampled_data.groupby(["id"]).apply(lambda x: x.loc[x.trade_biweek == a[0],"earning"].values[0] *3/6 + x.loc[x.trade_biweek == a[1],"earning"].values[0]*2/6 + x.loc[x.trade_biweek == a[2],"earning"].values[0]*1/6)

performance["load-300"] =( performance["earning_rank-300"] - performance["biggest_loss_rank-300"] )/(performance["volatility"] * np.sqrt(14))
performance["load-905"] =( performance["earning_rank-905"] - performance["biggest_loss_rank-905"] )/(performance["volatility"] * np.sqrt(14))
performance["load"] = np.maximum(performance["load-300"],performance["load-905"])

for m,v in {"sharp":1,"implied_volatility":2,"load":2}.items():
	performance[m + "_rank"] = performance[m].rank('min',ascending=True,pct = True)
	performance[m+"_adj"] = np.where(performance[m] > v,np.where(performance[m+"_rank"] <0.85, 0,
        np.where(((performance[m+"_rank"] >=0.85) & (performance[m+"_rank"] <0.9)), -0.05,
        np.where(((performance[m+"_rank"] >=0.9) & (performance[m+"_rank"] <0.95)), -0.10,
        np.where(((performance[m+"_rank"] >=0.95) & (performance[m+"_rank"] <0.99)), -0.15,
        np.where((performance[m+"_rank"] >=0.99), -0.25, 0))))),0)

performance["raw"] =np.add( np.add(performance["hf"],performance["sharp_adj"]),np.add(performance["implied_volatility_adj"],performance["load_adj"]))

performance_benchmark = performance.loc[GG_id].raw
performance["scaled"] = (performance["raw"] - performance_benchmark) * 100
#sampled_data.groupby("id").performance1.sum()
performance.to_csv("performance.csv",encoding='utf-8',index=True)
#data_result.to_csv("result.csv",encoding='utf-8')
