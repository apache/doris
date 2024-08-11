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
suite("test_db_name_ignore_cluster") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql "use ${db}"
    sql "drop table if exists ${db}.test_db_name_ignore_cluster"
    sql """create table ${db}.test_db_name_ignore_cluster(a int, b int) unique key(a) distributed by hash(a)
        properties("replication_num"="1");"""
    sql "select ${db}.test_db_name_ignore_cluster.a from `default_cluster:${db}`.test_db_name_ignore_cluster;"
    sql "select ${db}.test_db_name_ignore_cluster.* from `default_cluster:${db}`.test_db_name_ignore_cluster;"

    sql "select `default_cluster:${db}`.test_db_name_ignore_cluster.a from `${db}`.test_db_name_ignore_cluster;"
    sql "select `default_cluster:${db}`.test_db_name_ignore_cluster.* from `${db}`.test_db_name_ignore_cluster;"

    sql "select `default_cluster:${db}`.test_db_name_ignore_cluster.a from `default_cluster:${db}`.test_db_name_ignore_cluster;"
    sql "select `default_cluster:${db}`.test_db_name_ignore_cluster.* from `default_cluster:${db}`.test_db_name_ignore_cluster;"

    sql "select internal.`${db}`.test_db_name_ignore_cluster.a from internal.`default_cluster:${db}`.test_db_name_ignore_cluster;"
    sql "select internal.`default_cluster:${db}`.test_db_name_ignore_cluster.* from internal.`${db}`.test_db_name_ignore_cluster;"

    sql "insert into ${db}.test_db_name_ignore_cluster values(2,4)"
    sql "update `default_cluster:${db}`.test_db_name_ignore_cluster set `${db}`.test_db_name_ignore_cluster.b=10;"
    sql "update `${db}`.test_db_name_ignore_cluster set `${db}`.test_db_name_ignore_cluster.b=10;"
    qt_test_update "select * from ${db}.test_db_name_ignore_cluster"
}