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

suite("test_grant_col_priv") {
    String pwd = 'C123_567p'

    try_sql("drop user test_grant_col_priv_grantor")
    try_sql("drop user test_grant_col_priv_target")
    sql """drop database if exists test_grant_col_priv_db"""

    sql """create database test_grant_col_priv_db"""
    sql """
        create table test_grant_col_priv_db.test_grant_col_priv_tbl (c1 int, c2 int)
        duplicate key(c1)
        distributed by hash(c1) buckets 1
        properties ("replication_num" = "1")
        """
    sql """insert into test_grant_col_priv_db.test_grant_col_priv_tbl values (1, 2)"""

    sql """create user 'test_grant_col_priv_grantor' identified by '${pwd}'"""
    sql """create user 'test_grant_col_priv_target' identified by '${pwd}'"""
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO test_grant_col_priv_grantor"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO test_grant_col_priv_target"""
    }
    // for login
    sql """grant select_priv on regression_test to test_grant_col_priv_grantor"""
    sql """grant select_priv on regression_test to test_grant_col_priv_target"""

    // have grant_priv only, can not grant col select_priv
    sql """grant grant_priv on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_grantor"""
    connect('test_grant_col_priv_grantor', "${pwd}", context.config.jdbcUrl) {
        test {
            sql """grant select_priv(c1) on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_target"""
            exception "denied"
        }
        test {
            sql """grant select_priv(c1, c2) on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_grantor"""
            exception "denied"
        }
    }
    connect('test_grant_col_priv_target', "${pwd}", context.config.jdbcUrl) {
        test {
            sql """select c1 from test_grant_col_priv_db.test_grant_col_priv_tbl"""
            exception "denied"
        }
    }

    // have grant_priv and select_priv on c1, can grant/revoke select_priv on c1 but not on c2
    sql """grant select_priv(c1) on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_grantor"""
    connect('test_grant_col_priv_grantor', "${pwd}", context.config.jdbcUrl) {
        sql """grant select_priv(c1) on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_target"""
        test {
            sql """grant select_priv(c2) on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_target"""
            exception "denied"
        }
        test {
            sql """grant select_priv(c1, c2) on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_target"""
            exception "denied"
        }
        test {
            sql """revoke select_priv(c2) on test_grant_col_priv_db.test_grant_col_priv_tbl from test_grant_col_priv_target"""
            exception "denied"
        }
    }
    connect('test_grant_col_priv_target', "${pwd}", context.config.jdbcUrl) {
        order_qt_select_c1 """select c1 from test_grant_col_priv_db.test_grant_col_priv_tbl"""
        test {
            sql """select c2 from test_grant_col_priv_db.test_grant_col_priv_tbl"""
            exception "denied"
        }
    }
    connect('test_grant_col_priv_grantor', "${pwd}", context.config.jdbcUrl) {
        sql """revoke select_priv(c1) on test_grant_col_priv_db.test_grant_col_priv_tbl from test_grant_col_priv_target"""
    }
    connect('test_grant_col_priv_target', "${pwd}", context.config.jdbcUrl) {
        test {
            sql """select c1 from test_grant_col_priv_db.test_grant_col_priv_tbl"""
            exception "denied"
        }
    }

    // have grant_priv and select_priv on the table, can grant select_priv on any col
    sql """grant select_priv on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_grantor"""
    connect('test_grant_col_priv_grantor', "${pwd}", context.config.jdbcUrl) {
        sql """grant select_priv(c1, c2) on test_grant_col_priv_db.test_grant_col_priv_tbl to test_grant_col_priv_target"""
    }
    connect('test_grant_col_priv_target', "${pwd}", context.config.jdbcUrl) {
        order_qt_select_c1_c2 """select c1, c2 from test_grant_col_priv_db.test_grant_col_priv_tbl"""
    }
}
