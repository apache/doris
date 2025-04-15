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

suite("test_negative_default_column_value", "p0") {
    sql "drop table if exists test_negative_default_column_value"
    sql """CREATE TABLE test_negative_default_column_value 
        (col1 int NOT NULL,
        col2 int NULL default -1,
        col3 decimal NULL default -10.01
        )
        ENGINE=olap
        UNIQUE KEY(col1)
        DISTRIBUTED BY HASH (col1) BUCKETS 1 PROPERTIES('replication_num' = '1')
    """
    sql """insert into test_negative_default_column_value (col1) values (1)"""
    sql """insert into test_negative_default_column_value values (2, 2, 2.2)"""
    qt_col1 "select * from test_negative_default_column_value order by col1"
}
