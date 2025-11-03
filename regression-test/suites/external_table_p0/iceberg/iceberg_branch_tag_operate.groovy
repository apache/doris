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
        exception "main has no snapshot"
    }

    sql """ alter table test_branch_tag_operate create branch b1 """
    sql """ alter table test_branch_tag_operate create branch if not exists b1 """

    test {
        sql """ alter table test_branch_tag_operate create or replace branch b1 """
        exception "main has no snapshot"
    }

    test {
        sql """ alter table test_branch_tag_operate create branch b1 """
        exception "Ref b1 already exists"
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
        exception "Ref b7 already exists"
    }

    test {
        sql """ alter table test_branch_tag_operate create branch b8 as of version 11223344 """
        exception "Cannot set b8 to unknown snapshot: 11223344"
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
        exception "Ref t6 already exists"
    }

    test {
        sql """ alter table test_branch_tag_operate create branch t7 as of version 11223344 """
        exception "Cannot set t7 to unknown snapshot: 11223344"
    }

    // test branch/tag with schema change
    qt_sc01 """select * from tmp_schema_change_branch order by id;"""
    /// select by branch will use table schema
    qt_sc02 """select * from tmp_schema_change_branch@branch(test_branch) order by id;;"""
    qt_sc03 """select * from tmp_schema_change_branch for version as of "test_branch" order by id;;"""
    List<List<Object>> refs = sql """select * from tmp_schema_change_branch\$refs order by name"""
    String s_main = refs.get(0)[2]
    String s_test_branch = refs.get(1)[2]
    
    /// select by version will use branch schema
    qt_sc04 """SELECT * FROM tmp_schema_change_branch for VERSION AS OF ${s_test_branch} order by id;"""
    qt_sc05 """SELECT * FROM tmp_schema_change_branch for VERSION AS OF ${s_main} order by id;"""

    /// select by tag will use tag schema
    qt_sc06 """SELECT * FROM tmp_schema_change_branch@tag(test_tag) order by id;"""

    // ----------------------------------------------------------------------------------------
    // test drop branch / tag
    // ----------------------------------------------------------------------------------------

    test {
        sql """ alter table test_branch_tag_operate drop branch if exists t2 """
        exception "Ref t2 is a tag not a branch"
    }

    test {
        sql """ alter table test_branch_tag_operate drop branch t2 """
        exception "Ref t2 is a tag not a branch"
    }

    test {
        sql """ alter table test_branch_tag_operate drop tag if exists b2 """
        exception "Ref b2 is a branch not a tag"
    }

    test {
        sql """ alter table test_branch_tag_operate drop tag b2 """
        exception "Ref b2 is a branch not a tag"
    }

    sql """ alter table test_branch_tag_operate drop branch if exists not_exists_branch """
    test {
        sql """ alter table test_branch_tag_operate drop branch not_exists_branch """
        exception "Branch does not exist: not_exists_branch"
    }

    sql """ alter table test_branch_tag_operate drop tag if exists not_exists_tag """
    test {
        sql """ alter table test_branch_tag_operate drop tag not_exists_tag """
        exception "Tag does not exist: not_exists_tag"
    }

    // drop tag success, then read
    sql """ alter table test_branch_tag_operate drop tag t2 """
    sql """ alter table test_branch_tag_operate drop tag if exists t3 """
    test {
        sql """ select * from test_branch_tag_operate@tag(t2) """
        exception "does not have tag named t2"
    }
    test {
        sql """ select * from test_branch_tag_operate@tag(t3) """
        exception "does not have tag named t3"
    }

    // drop branch success, then read
    sql """ alter table test_branch_tag_operate drop branch b2 """
    sql """ alter table test_branch_tag_operate drop branch if exists b3 """
    test {
        sql """ select * from test_branch_tag_operate@branch(b2) """
        exception "does not have branch named b2"
    }
    test {
        sql """ select * from test_branch_tag_operate@branch(b3) """
        exception "does not have branch named b3"
    }
}
