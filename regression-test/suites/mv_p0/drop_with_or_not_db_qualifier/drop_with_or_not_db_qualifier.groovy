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

suite ("drop_with_or_not_db_qualifier") {
    sql "set enable_fallback_to_original_planner = false"

    String db = context.config.getDbNameByFile(context.file)

    sql"""drop database if exists ${db} """
    sql """create database ${db}"""

    sql """drop database if exists ${db}_another"""
    sql """create database ${db}_another"""
    sql "use ${db}_another"

    sql """drop table if exists ${db}.t;"""
    sql """drop table if exists ${db}.t1;"""


    sql """
    CREATE TABLE ${db}.t (
        id int null,
        k largeint null,
        d date null,
        k1 int
    )
    PARTITION BY RANGE (`id`) (
        partition p1 values [(1), (2))
    )
    DISTRIBUTED BY HASH(`k`, `id`) BUCKETS 16
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    sql "create materialized view ${db}.mv_t as select d from ${db}.t;"
    Thread.sleep(2000)

    // drop column should success when create and drop in another db
    sql """alter table ${db}.t drop column k1;"""

    //
    sql """use ${db}"""

    sql """
    CREATE TABLE t1 (
        id int null,
        k largeint null,
        d date null,
        k1 int
    )
    PARTITION BY RANGE (`id`) (
        partition p1 values [(1), (2))
    )
    DISTRIBUTED BY HASH(`k`, `id`) BUCKETS 16
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    sql """alter table ${db}.t1 ADD ROLLUP mv_t1(d); """

    Thread.sleep(2000)
    sql """use ${db}_another"""

    // should success when create in one db and drop in another db
    sql """ alter table ${db}.t1 drop column k1;"""

}
