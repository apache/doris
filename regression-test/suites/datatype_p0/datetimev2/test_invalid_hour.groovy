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

suite("test_invalid_hour") {
    qt_sql """
        select cast("2024-02-02 24:23:12" as DateTime);
    """
    qt_sql """
        select cast(concat("2024-02-02", ' ', "24:23:12") as DateTime);
    """
    sql "drop table if exists test_invalid_hour_null"
    sql "drop table if exists test_invalid_hour_not_null"
    sql """
        create table test_invalid_hour_null (
            `rowid` int,
            `str` varchar,
            `dt` datetime null
        ) ENGINE=OLAP
        UNIQUE KEY(`rowid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`rowid`) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "lineitem_orders",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        create table test_invalid_hour_not_null (
            `rowid` int,
            `str` varchar,
            `dt` datetime not null
        ) ENGINE=OLAP
        UNIQUE KEY(`rowid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`rowid`) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "lineitem_orders",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        insert into test_invalid_hour_null values
            (2, "2023-12-12 23:00:00", "2023-12-12 23:00:00")
    """

    sql """
        insert into test_invalid_hour_not_null values 
            (1, "2023-12-12 23:00:00", "2023-12-12 23:00:00")
    """

    test {
        sql """ set enable_insert_strict = true; """
        
        sql """
            insert into test_invalid_hour_not_null values
                (2, "2023-12-12 24:00:00", "2023-12-12 24:00:00")
        """
        exception ""

    }
    
    qt_sql """
        select *, cast(str as Datetime) from test_invalid_hour_null order by rowid;
    """

    qt_sql """
        select *, cast(str as Datetime) from test_invalid_hour_not_null order by rowid;
    """
}