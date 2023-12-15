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

suite("minus_except") {
    def tableNameVarchar = "test_varchar"
    def tableNameDate = "test_date"
    sql "DROP TABLE IF EXISTS ${tableNameVarchar}"
    sql "DROP TABLE IF EXISTS ${tableNameDate}"
    sql "CREATE TABLE ${tableNameVarchar}(c1 varchar) DUPLICATE key(c1) DISTRIBUTED BY hash(c1) BUCKETS auto PROPERTIES ('replication_num' = '1')"
    sql "CREATE TABLE ${tableNameDate}(c2 date) DUPLICATE key(c2) DISTRIBUTED BY hash(c2) BUCKETS auto PROPERTIES ('replication_num' = '1')"
    sql "insert into ${tableNameVarchar} values('+06-00');"
    sql "insert into ${tableNameDate} values('1990-11-11');"
    qt_select " SELECT c1,c1 FROM ${tableNameVarchar} EXCEPT SELECT c2,c2 FROM ${tableNameDate};"
    sql "DROP TABLE IF EXISTS ${tableNameVarchar}"
    sql "DROP TABLE IF EXISTS ${tableNameDate}"
}