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

suite("prepared_show") {
    def tableName = "prepared_show"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    sql """drop database if exists prepared_show"""
    sql """create database prepared_show"""
    sql """use prepared_show"""
    sql """CREATE TABLE IF NOT EXISTS prepared_show_table1 (`k1` tinyint NULL)
           ENGINE=OLAP
           DUPLICATE KEY(`k1`)
           DISTRIBUTED BY HASH(`k1`) BUCKETS 1
           PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
           )"""

    sql """CREATE TABLE IF NOT EXISTS prepared_show_table2 (`k1` tinyint NULL)
           ENGINE=OLAP
           DUPLICATE KEY(`k1`)
           DISTRIBUTED BY HASH(`k1`) BUCKETS 1
           PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
           )"""
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "prepared_show")
    def result1 = connect(user, password, url) {
        def stmt_read = prepareStatement """show databases like "prepared_show" """
        qe_stmt_show_db stmt_read

        stmt_read = prepareStatement """show tables from prepared_show"""
        qe_stmt_show_table stmt_read

        stmt_read = prepareStatement """show table stats prepared_show_table1"""
        qe_stmt_show_table_stats1 stmt_read

        stmt_read = prepareStatement """show processlist"""
        stmt_read.executeQuery()
    }
}
