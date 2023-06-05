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

suite("test_cast_decimal_to_boolean") {
    def tableName = "tbl_test_decimal_to_boolean"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "CREATE  TABLE if NOT EXISTS ${tableName} (c0 BOOLEAN NOT NULL) DUPLICATE KEY(c0) DISTRIBUTED BY HASH (c0) BUCKETS 1 PROPERTIES ('replication_num' = '1');"
    sql """insert into ${tableName} values  (1),
										  	(0),
										  	(0)"""

    qt_select " select (c0 BETWEEN 0.44 AND 1) from ${tableName} order by 1"

    sql "DROP TABLE IF EXISTS ${tableName}"
}
