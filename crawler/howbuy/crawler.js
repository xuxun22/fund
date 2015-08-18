#!/usr/bin/env nodejs

var http = require('http');
var util = require('util');
var csv = require('ya-csv');
var sprintf = require('sprintf-js').sprintf;
var file = require('fs');
var window = {};

var product_url = "http://static.howbuy.com/min/f=/js/data/pfundJson_v2881.js";
var base_url = "http://static.howbuy.com/min/f=/upload/auto/script/fund/smhb_%s_v160.js";

function get_product_wrapper(p) {
	file.stat('data/'+p+'.csv',function(err,stat){
		if (err == null){
			console.log(p + " exists!");
		} else if (err.code == 'ENOENT') {
			get_product(p);
		}
	});
}

function get_product(product){
	//			var product = ret[pi].code;
	console.log(product);
	var url = sprintf(base_url, product);
	http.get(url, function(res) {
		//  console.log("statusCode: ", res.statusCode);
		//	console.log("headers: ", res.headers);
		var data = "";
		res.on('data', function(d) {
			data += d.toString();
		});
		res.on('end',function() {
			console.log("callbacking with "+product);
			try {
				var ret = eval(data+"JzzsDateObj");
				var file_content = [];
				for (line in ret.navList) {
					var content = ret.navList[line].split(",");
					var filter_content = new Array();
					filter_content[0] = sprintf("%04d-%02d-%02d",parseInt(content[0]),parseInt(content[1])+1,parseInt(content[2]));
					filter_content[1] = parseFloat(content[6]) / ret.jzdw[0];
					filter_content[2] = product; 

					file_content.push(filter_content.join(","))
				}
				file.writeFile("data/" + product+".csv.tmp",file_content.join("\n"));
				file.rename("data/" + product+".csv.tmp","data/" + product+".csv");


//				util.inspect(ret.navList);
//				var writer = new csv.createCsvFileWriter("data/" + product+".csv");
//				for (line in ret.navList) {
//					var content = ret.navList[line].split(",");
//					var filter_content = new Array();
//					filter_content[0] = sprintf("%04d-%02d-%02d",parseInt(content[0]),parseInt(content[1])+1,parseInt(content[2]));
//					filter_content[1] = parseFloat(content[6]) / 100;
//					filter_content[2] = product; 
//
//					writer.writeRecord(filter_content);
//				}
////				writer.end();
			}
			catch(e) {
				console.log("Hitting some problem with " + product +":" + e);
			}
		});

	}).on('error', function(e) {
		console.error(e);
	});
}

// get_product("P00134")

http.get(product_url, function(res) {
	//  console.log("statusCode: ", res.statusCode);
	//	console.log("headers: ", res.headers);
	var products = "";
	res.on('data', function(d) {
		products += d.toString();
		console.log("got some data...")
	});
	res.on('end',function() {
		console.log("all products ...")
		ret = eval(products +"window.product_pfund");
		for (pi in ret) {
			var p = ret[pi].code;
			get_product_wrapper(p);
		}
	});

}).on('error', function(e) {
	console.error(e);
});

