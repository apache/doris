/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
		"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
**/

var mysql = require("mysql");

//connect to doris
var conn = mysql.createConnection({
	host: "127.0.0.1",
	user: "root",
	password: "",
	//端口为fe.conf中的query_port
	port: 9030,
});
conn.connect(function(err){
	if (err) {
		console.log("connect to doris failed: " + err);
	} else {
		console.log("connect to doris successfully");
	}
});

//create database
conn.query("CREATE DATABASE IF NOT EXISTS db_test", function(err){
	if (err) {
		console.log("create database failed: " + err);
	} else {
		console.log("create database successfully");
	}
});

//set db context
conn.query("USE db_test", function(err){
	if (err) {
		console.log("set db context failed: " + err);
	} else {
		console.log("set db context successfully");
	}
});

//create table
var sql = "CREATE TABLE IF NOT EXISTS table_test(siteid INT, citycode SMALLINT, pv BIGINT SUM) " +
	"AGGREGATE KEY(siteid, citycode) " +
	"DISTRIBUTED BY HASH(siteid) BUCKETS 10 " +
	"PROPERTIES(\"replication_num\" = \"1\")";
conn.query(sql, function(err){
	if (err) {
		console.log("create table failed: " + err);
	} else {
		console.log("create table successfully");
	}
});

//insert data
sql = "INSERT INTO table_test values(1, 2, 3), (4, 5, 6), (1, 2, 4)"
conn.query(sql, function(err){
	if (err) {
		console.log("insert data failed: " + err);
	} else {
		console.log("insert data successfully");
	}
});

//query data
conn.query("SELECT * FROM table_test", function(err, result){
	if (err) {
		console.log("query data failed: " + err);
		return;
	}

	console.log("query data successfully");
	console.log("siteid\tcitycode\tpv");
	result.forEach(function(row){
		console.log(row.siteid + "\t" + row.citycode + "\t" + row.pv);
	});
});

//drop database
conn.query("DROP DATABASE IF EXISTS db_test", function(err){
	if (err) {
		console.log("drop database failed: ", err);
	} else {
		console.log("drop database successfully");
	}
});
