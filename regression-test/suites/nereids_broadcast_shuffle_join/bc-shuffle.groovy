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

suite("bc-shuffle") {
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql "set enable_parallel_result_sink=false;"
    String database = context.config.getDbNameByFile(context.file)
    sql "drop database if exists ${database}"
    sql "create database ${database}"
    sql "use ${database}"
    sql """
    drop table if exists t1;
    """

    sql """
    create table t1(
    id int,
    code varchar(50)
    )ENGINE=OLAP
    duplicate key (id)
    distributed by hash(id) buckets 10
    properties(
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    drop table if exists t2;
    """
    sql """
    create table t2(
        id int,
        ACCEPT_ORG_CODE varchar(50)
    )ENGINE=OLAP
    duplicate key (id)
    distributed by hash(id) buckets 10
    properties(
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "set enable_nereids_planner=true"
    sql "set forbid_unknown_col_stats=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set runtime_filter_type=4"
    sql """
    alter table t1 modify column code set stats('row_count'='7846', 'ndv'='10000', 'num_nulls'='0', 'min_value'='999999', 'max_value'='999999', 'data_size'='5.7');
    """
    sql """
    alter table t2 modify column ACCEPT_ORG_CODE set stats('row_count'='1033416', 'ndv'='10000', 'num_nulls'='0', 'min_value'='999999', 'max_value'='999999', 'data_size'='5.7');
    """

    qt_bc1 """
    explain shape plan select * from t1 join t2 on code = ACCEPT_ORG_CODE;
    """

    qt_bc2 """
    explain shape plan select * from t2 left join t1 on code = ACCEPT_ORG_CODE;
    """

    sql """alter table t1 modify column code set stats('row_count'='2000000', 'ndv'='10000', 'num_nulls'='0', 'min_value'='999999', 'max_value'='999999', 'data_size'='12');"""
    sql """alter table t2 modify column ACCEPT_ORG_CODE set stats('row_count'='20000000000', 'ndv'='10000', 'num_nulls'='0', 'min_value'='999999', 'max_value'='999999', 'data_size'='12');"""

    qt_bc3 """
    explain shape plan select * from t1 join t2 on code = ACCEPT_ORG_CODE;
    """

    qt_bc4 """
    explain shape plan select * from t2 left join t1 on code = ACCEPT_ORG_CODE;
    """
}