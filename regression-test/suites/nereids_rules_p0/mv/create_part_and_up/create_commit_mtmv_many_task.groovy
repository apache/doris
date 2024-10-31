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

import java.time.LocalDate

suite("create_commit_mtmv_many_tasks", "p2") {

    def src_database_name = context.config.getDbNameByFile(context.file)
    sql """drop database if exists ${src_database_name};"""
    sql """create database ${src_database_name};"""
    sql """use ${src_database_name};"""

    def table_name1 = "lineitem"
    def table_name2 = "orders"
    sql """drop table if exists ${table_name1}"""
    sql """drop table if exists ${table_name2}"""
    sql """CREATE TABLE lineitem (
        l_orderkey    bigint NOT NULL,
        l_linenumber  int not null,
        l_partkey     int NOT NULL,
        l_suppkey     int not null,
        l_quantity    decimal(15, 2) NOT NULL,
        l_extendedprice  decimal(15, 2) NOT NULL,
        l_discount    decimal(15, 2) NOT NULL,
        l_tax         decimal(15, 2) NOT NULL,
        l_returnflag  VARCHAR(1) NOT NULL,
        l_linestatus  VARCHAR(1) NOT NULL,
        l_shipdate    DATE NOT NULL,
        l_commitdate  DATE NOT NULL,
        l_receiptdate DATE NOT NULL,
        l_shipinstruct VARCHAR(25) NOT NULL,
        l_shipmode     VARCHAR(10) NOT NULL,
        l_comment      VARCHAR(44) NOT NULL,
        l_null VARCHAR(1) NULL
    )ENGINE=OLAP
    UNIQUE KEY(`l_orderkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "lineitem_orders",
        "enable_unique_key_merge_on_write" = "true"
    );"""
    sql """CREATE TABLE orders  (
        o_orderkey       bigint NOT NULL,
        o_custkey        int NOT NULL,
        o_orderstatus    VARCHAR(1) NOT NULL,
        o_totalprice     decimal(15, 2) NOT NULL,
        o_orderdate      DATE NOT NULL,
        o_orderpriority  VARCHAR(15) NOT NULL,
        o_clerk          VARCHAR(15) NOT NULL,
        o_shippriority   int NOT NULL,
        o_comment        VARCHAR(79) NOT NULL,
        o_null VARCHAR(1) NULL
    )ENGINE=OLAP
    UNIQUE KEY(`o_orderkey`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
        "replication_num" = "1",
        "colocate_with" = "lineitem_orders",
        "enable_unique_key_merge_on_write" = "false"
    );"""

    def stream_load_job = { table_name, src_file_name ->
        streamLoad {
            table table_name
            set 'column_separator', '|'
            file """${getS3Url() + '/regression/tpch/sf1/'}${src_file_name}"""

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }

        }
        sql "select count(*) from ${table_name}"
    }

    for (int i = 1; i <= 10; i++) {
        stream_load_job(table_name1, "lineitem.tbl.${i}")
        stream_load_job(table_name2, "orders.tbl.${i}")
    }

    def dst_database_name = "wz_tpch_mtmv_hit_property"
    sql """drop database if exists ${dst_database_name};"""
    sql """create database ${dst_database_name};"""
    sql """use ${dst_database_name};"""

    sql """drop table if exists ${table_name1}"""
    sql """drop table if exists ${table_name2}"""
    sql """CREATE TABLE `lineitem` (
          `l_orderkey` BIGINT NOT NULL,
          `l_linenumber` INT NOT NULL,
          `l_partkey` INT NOT NULL,
          `l_suppkey` INT NOT NULL,
          `l_quantity` DECIMAL(15, 2) NOT NULL,
          `l_extendedprice` DECIMAL(15, 2) NOT NULL,
          `l_discount` DECIMAL(15, 2) NOT NULL,
          `l_tax` DECIMAL(15, 2) NOT NULL,
          `l_returnflag` VARCHAR(1) NOT NULL,
          `l_linestatus` VARCHAR(1) NOT NULL,
          `l_commitdate` DATE NOT NULL,
          `l_receiptdate` DATE NOT NULL,
          `l_shipinstruct` VARCHAR(25) NOT NULL,
          `l_shipmode` VARCHAR(10) NOT NULL,
          `l_comment` VARCHAR(44) NOT NULL,
          `l_shipdate` DATE NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
        COMMENT 'OLAP'
        AUTO PARTITION BY range(date_trunc(`l_shipdate`, 'day')) ()
        DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""
    sql """CREATE TABLE `orders` (
      `o_orderkey` BIGINT NOT NULL,
      `o_custkey` INT NOT NULL,
      `o_orderstatus` VARCHAR(1) NOT NULL,
      `o_totalprice` DECIMAL(15, 2) NOT NULL,
      `o_orderpriority` VARCHAR(15) NOT NULL,
      `o_clerk` VARCHAR(15) NOT NULL,
      `o_shippriority` INT NOT NULL,
      `o_comment` VARCHAR(79) NOT NULL,
      `o_orderdate` DATE NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    AUTO PARTITION BY range(date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""
    sql """drop MATERIALIZED VIEW if exists mv1;"""
    sql """
        CREATE MATERIALIZED VIEW mv1
        BUILD IMMEDIATE REFRESH ON COMMIT
        partition by(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') AS
        select l_shipdate, l_orderkey from lineitem as t1 left join orders as t2 on t1.l_orderkey = t2.o_orderkey group by l_shipdate, l_orderkey;
        """

    def insert_into_select = { date_it ->
        sql """INSERT INTO ${dst_database_name}.${table_name1}
            SELECT l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, '${date_it}' AS new_date_column
            FROM ${src_database_name}.${table_name1};"""

        sql """INSERT INTO ${dst_database_name}.${table_name2}
            SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderpriority, o_clerk, o_shippriority, o_comment, '${date_it}' AS new_date_column
            FROM ${src_database_name}.${table_name2}"""
    }

    def get_next_day = { def date_it ->
        def date = LocalDate.parse(date_it)
        def next_day = date.plusDays(1)
        return next_day
    }

    def start_date = "2023-12-01"
    while (true) {
        if (start_date.toString() == "2024-03-11") {
            break
        }
        logger.info("task load start")
        insert_into_select(start_date)
        start_date = get_next_day(start_date.toString())
    }

    def job_name = getJobName(dst_database_name, "mv1")
    waitingMTMVTaskFinished(job_name)
    def task_num = sql """select count(*) from tasks("type"="mv") where JobName="${job_name}";"""
    assertTrue(task_num[0][0] < 100)

    def mv_row_count = sql """select count(1) from mv1;"""
    def real_row_count = sql """select count(1) from (select l_shipdate, l_orderkey from lineitem as t1 left join orders as t2 on t1.l_orderkey = t2.o_orderkey group by l_shipdate, l_orderkey) t;"""
    assertTrue(mv_row_count[0][0] == real_row_count[0][0])

}
