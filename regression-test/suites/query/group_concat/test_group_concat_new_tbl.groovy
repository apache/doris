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

suite("test_group_concat_new_tbl", "issue_7288") {
    def leftTableName = "group_concat_i7288_left"
    def rightTableName = "group_concat_i7288_right"
    sql "DROP TABLE IF EXISTS ${leftTableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${leftTableName} (
            k1 int,
            no varchar(10) NOT NULL
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """
    sql "INSERT INTO ${leftTableName} values(1,'test')"

    sql "DROP TABLE IF EXISTS ${rightTableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${rightTableName} (
            k1 int,
            no varchar(10) NOT NULL
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """

    sql "set enable_vectorized_engine=true"
    qt_select(""" select l.k1, group_concat(r.no) from ${leftTableName} l left join ${rightTableName} r on l.k1=r.k1 group by l.k1; """);

    sql "set enable_vectorized_engine=false"
    qt_select(""" select l.k1, group_concat(r.no) from ${leftTableName} l left join ${rightTableName} r on l.k1=r.k1 group by l.k1; """);
}