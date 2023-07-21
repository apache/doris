// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_decimalv3") {
    def db = "test_decimalv3_db"
    sql "CREATE DATABASE IF NOT EXISTS ${db}"
    sql "use ${db}"
    sql "drop table if exists test5"
	sql '''CREATE  TABLE test5 (   `a` decimalv3(38,18),   `b` decimalv3(38,18) ) ENGINE=OLAP DUPLICATE KEY(`a`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`a`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1" ) '''
	sql "insert into test5 values(50,2)"
	sql "drop view if exists test5_v"
	sql "create view test5_v (amout) as select cast(a*b as decimalv3(38,18)) from test5"

	qt_decimalv3 "select * from test5_v"
	qt_decimalv3 "select cast(a as decimalv3(12,10)) * cast(b as decimalv3(18,10)) from test5"
}
