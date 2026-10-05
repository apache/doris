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

suite("test_agg_case_sensitive") {
    String suiteName = "test_agg_case_sensitive"
    String tableName = "${suiteName}_table"

   sql """drop table if exists `${tableName}`"""

    sql """ set enable_agg_state=true; """
    // upper case
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            k2 AGG_STATE<MAX_BY(int not null,int)> generic
        )
        ENGINE=OLAP
        AGGREGATE KEY (id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");
        """

    sql """
        insert into ${tableName} values(1,max_by_state(3,3));
        """

    qt_sql_upper """select id,max_by_merge(k2) from ${tableName} group by id"""

    sql """drop table if exists `${tableName}`"""

    // lower case
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            k2 AGG_STATE<max_by(int not null,int)> generic
        )
        ENGINE=OLAP
        AGGREGATE KEY (id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES ("replication_num" = "1");
        """

    sql """
        insert into ${tableName} values(1,max_by_state(3,3));
        """

    qt_sql_lower """select id,max_by_merge(k2) from ${tableName} group by id"""

    sql """drop table if exists `${tableName}`"""
}
