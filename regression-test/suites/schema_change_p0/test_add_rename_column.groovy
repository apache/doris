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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("test_add_rename_column") {
    def tableName = "test_add_rename"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        create table ${tableName}(
            id int,
            name varchar(100)
        ) ENGINE = olap
        unique key(id)
        distributed by hash(id) buckets 1
        properties (
            'replication_num' = 1
        )
    """

    sql """ insert into ${tableName} values (2, 'bb') """

    sql """ ALTER TABLE ${tableName} add column c3 varchar(10) """

    sql """ ALTER TABLE ${tableName} rename column c3 c4 """

    sql """ truncate table ${tableName} """

    sql """ sync """

    qt_sql """ select * from ${tableName} """

    sql """ insert into ${tableName} values (3, 'cc', 'dd') """

    sql """ sync """

    qt_sql """ select * from ${tableName} """
}
