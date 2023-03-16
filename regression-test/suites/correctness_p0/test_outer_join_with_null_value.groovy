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

suite("test_outer_join_with_null_value") {
    sql """
        drop table if exists outer_table_a;
    """

    sql """
        drop table if exists outer_table_b;
    """
    
    sql """
        create table outer_table_a
        (
            PROJECT_ID VARCHAR(32) not null,
            SO_NO VARCHAR(32) not null,
            ORG_ID VARCHAR(32) not null
        )ENGINE = OLAP
        DUPLICATE KEY(PROJECT_ID)
        DISTRIBUTED BY HASH(PROJECT_ID) BUCKETS 30
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        create table outer_table_b
        (
            PROJECT_ID VARCHAR(32) not null,
            SO_NO VARCHAR(32),
            ORG_ID VARCHAR(32) not null
        )ENGINE = OLAP
        DUPLICATE KEY(PROJECT_ID)
        DISTRIBUTED BY HASH(PROJECT_ID) BUCKETS 30
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into outer_table_a values('1','1','1');
    """

    sql """
        insert into outer_table_b values('1','1','1'),('1',null,'1');
    """

    qt_select1 """
        select
        count(*)
        FROM
        outer_table_b WSA
        LEFT JOIN outer_table_a WBWD ON WBWD.ORG_ID = WSA.ORG_ID
        AND WBWD.PROJECT_ID = WSA.PROJECT_ID
        AND WBWD.SO_NO = WSA.SO_NO;
    """

    qt_select2 """
        select
        count(*)
        FROM
        outer_table_b WSA
        LEFT JOIN outer_table_a WBWD ON WBWD.ORG_ID = WSA.ORG_ID
        AND WBWD.PROJECT_ID = WSA.PROJECT_ID
        AND WBWD.SO_NO = WSA.SO_NO
        AND WBWD.SO_NO >= WSA.SO_NO;
    """

    sql """
        drop table if exists outer_table_a;
    """

    sql """
        drop table if exists outer_table_b;
    """
}
