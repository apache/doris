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

suite("iceberg_branch_tag_operate", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_tag_operate"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """ use ${catalog_name}.test_db """

    sql """ drop table if exists test_branch_tag_operate """
    sql """ create table test_branch_tag_operate (id int) """

    // with empty table

    test {
        sql """ alter table test_branch_tag_operate create tag b1 """
        exception "Exception"
    }

    sql """ alter table test_branch_tag_operate create branch b1 """
    sql """ alter table test_branch_tag_operate create branch if not exists b1 """

    test {
        sql """ alter table test_branch_tag_operate create or replace branch b1 """
        exception "Exception"
    }

    test {
        sql """ alter table test_branch_tag_operate create branch b1 """
        exception "Exception"
    }

    qt_q1 """ select * from test_branch_tag_operate@branch(b1) """ // empty table

    // with some data
    sql """ insert into test_branch_tag_operate values (1) """
    sql """ insert into test_branch_tag_operate values (2) """
    sql """ insert into test_branch_tag_operate values (3) """
    sql """ insert into test_branch_tag_operate values (4) """
    sql """ insert into test_branch_tag_operate values (5) """

    
    List<List<Object>> snapshots = sql """ select snapshot_id from iceberg_meta("table" = "${catalog_name}.test_db.test_branch_tag_operate", "query_type" = "snapshots") order by committed_at; """
    String s0 = snapshots.get(0)[0]
    String s1 = snapshots.get(1)[0]
    String s2 = snapshots.get(2)[0]
    String s3 = snapshots.get(3)[0]
    String s4 = snapshots.get(4)[0]

    // branch
    sql """ alter table test_branch_tag_operate create branch b2 as of version ${s0} """
    qt_q2 """ select * from test_branch_tag_operate@branch(b2) order by id """ // 0 records

    sql """ alter table test_branch_tag_operate create or replace branch b2 AS OF VERSION ${s1} RETAIN 2 days """
    qt_q3 """ select * from test_branch_tag_operate@branch(b2) order by id """ // 1 records

    sql """ alter table test_branch_tag_operate create or replace branch b2 AS OF VERSION ${s2} RETAIN 2 hours WITH SNAPSHOT RETENTION 3 SNAPSHOTS"""
    qt_q4 """ select * from test_branch_tag_operate@branch(b2) order by id """ // 2 records

    sql """ alter table test_branch_tag_operate replace branch b2 AS OF VERSION ${s3} RETAIN 2 hours WITH SNAPSHOT RETENTION 4 DAYS """
    qt_q5 """ select * from test_branch_tag_operate@branch(b2) order by id """ // 3 records

    sql """ alter table test_branch_tag_operate create or replace branch b2 RETAIN 2 hours WITH SNAPSHOT RETENTION 3 SNAPSHOTS 4 DAYS """
    qt_q6 """ select * from test_branch_tag_operate@branch(b2) order by id """ // 5 records

    sql """ alter table test_branch_tag_operate create or replace branch b3 AS OF VERSION ${s1} RETAIN 2 days """
    qt_q7 """ select * from test_branch_tag_operate@branch(b3) order by id """ // 1 records

    sql """ alter table test_branch_tag_operate create branch if not exists b3 AS OF VERSION ${s2} RETAIN 2 days """
    qt_q8 """ select * from test_branch_tag_operate@branch(b3) order by id """ // still 1 records

    sql """ alter table test_branch_tag_operate create branch if not exists b4 AS OF VERSION ${s2} RETAIN 2 MINUTES WITH SNAPSHOT RETENTION 3 SNAPSHOTS """
    qt_q9 """ select * from test_branch_tag_operate@branch(b4) order by id """ // 2 records

    sql """ alter table test_branch_tag_operate create branch if not exists b5 """
    qt_q10 """ select * from test_branch_tag_operate@branch(b5) order by id """ // 5 records

    sql """ alter table test_branch_tag_operate create branch if not exists b6 AS OF VERSION ${s2} """
    qt_q11 """ select * from test_branch_tag_operate@branch(b6) order by id """ // 2 records

    sql """ alter table test_branch_tag_operate create or replace branch b6 AS OF VERSION ${s3} """
    qt_q12 """ select * from test_branch_tag_operate@branch(b6) order by id """ // 3 records

    sql """ alter table test_branch_tag_operate create or replace branch b6 """
    qt_q13 """ select * from test_branch_tag_operate@branch(b6) order by id """ // 5 records

    sql """ alter table test_branch_tag_operate create or replace branch b6 """
    qt_q14 """ select * from test_branch_tag_operate@branch(b6) order by id """ // still 5 records

    sql """ alter table test_branch_tag_operate create or replace branch b6 RETAIN 2 DAYS """
    qt_q15 """ select * from test_branch_tag_operate@branch(b6) order by id """ // still 5 records

    sql """ alter table test_branch_tag_operate create branch b7 """
    qt_q16 """ select * from test_branch_tag_operate@branch(b7) order by id """ // 5 records

    test {
        sql """ alter table test_branch_tag_operate create branch b7 as of version ${s3} """
        exception "Exception"
    }

    test {
        sql """ alter table test_branch_tag_operate create branch b8 as of version 11223344} """
        exception "Exception"
    }


    // tag
    sql """ alter table test_branch_tag_operate create tag t2 as of version ${s0} """
    qt_q20 """ select * from test_branch_tag_operate@tag(t2) order by id """ // 0 records

    sql """ alter table test_branch_tag_operate create or replace tag t2 as of version ${s1} """
    qt_q21 """ select * from test_branch_tag_operate@tag(t2) order by id """ // 1 records

    sql """ alter table test_branch_tag_operate create or replace tag t2 as of version ${s2} RETAIN 10 MINUTES """
    qt_q22 """ select * from test_branch_tag_operate@tag(t2) order by id """ // 2 records

    sql """ alter table test_branch_tag_operate create or replace tag t2 RETAIN 10 MINUTES """
    qt_q23 """ select * from test_branch_tag_operate@tag(t2) order by id """ // 5 records

    sql """ alter table test_branch_tag_operate create tag if not exists t3 as of version ${s1} """
    qt_q24 """ select * from test_branch_tag_operate@tag(t3) order by id """ // 1 records

    sql """ alter table test_branch_tag_operate create tag if not exists t3 as of version ${s2} """  // still 1 records
    qt_q25 """ select * from test_branch_tag_operate@tag(t3) order by id """

    sql """ alter table test_branch_tag_operate create tag t4 as of version ${s2} """
    qt_q26 """ select * from test_branch_tag_operate@tag(t4) order by id """ // 2 records

    sql """ alter table test_branch_tag_operate create or replace tag t5 as of version ${s3} """
    qt_q27 """ select * from test_branch_tag_operate@tag(t5) order by id """ // 3 records

    sql """ alter table test_branch_tag_operate create tag t6 """
    qt_q28 """ select * from test_branch_tag_operate@tag(t6) order by id """ // 5 records

    test {
        sql """ alter table test_branch_tag_operate create tag t6 as of version ${s3} """
        exception "Exception"
    }

    test {
        sql """ alter table test_branch_tag_operate create branch t7 as of version 11223344} """
        exception "Exception"
    }

}
