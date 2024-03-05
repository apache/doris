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

suite('nereids_insert_auth') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    def db = 'nereids_insert_auth_db'
    sql "drop database if exists ${db}"
    sql "create database ${db}"
    sql "use ${db}"

    def t1 = 't1'

    sql "drop table if exists ${t1}"

    sql """
        create table ${t1} (
            id int,
            c1 bigint
        )
        distributed by hash(id) buckets 2
        properties(
            'replication_num'='1'
        );
    """

    String user = "nereids_insert_auth_user";
    String pwd = '123456';
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    connect(user=user, password="${pwd}", url=url) {
        try {
            sql """ insert into ${db}.${t1} values (1, 1) """
            fail()
        } catch (Exception e) {
            log.info(e.getMessage())
        }
    }

    sql """GRANT LOAD_PRIV ON ${db}.${t1} TO ${user}"""

    connect(user=user, password="${pwd}", url=url) {
        try {
            sql """ insert into ${db}.${t1} values (1, 1) """
        } catch (Exception e) {
            log.info(e.getMessage())
            fail()
        }
    }

    connect(user=user, password="${pwd}", url=url) {
        try {
            sql """ insert overwrite table ${db}.${t1} values (2, 2) """
        } catch (Exception e) {
            log.info(e.getMessage())
            fail()
        }
    }
}