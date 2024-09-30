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

import org.junit.Assert

suite("mtmv_life_lock_test") {

    String db = context.config.getDbNameByFile(context.file)

    def lineitem_table = "lineitem_1"
    def orders_table = "orders_1"
    def partsupp_table = "partsupp_1"
    def lineitem_table_new = "lineitem_1_new"
    def mv_name = "mtmv_life_lock1"
    def mv_name_new = "mtmv_life_lock1_new"
    def mv_name2 = "mtmv_life_lock_level2"
    def mtmv_sql = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, l_orderkey, l_linenumber, o_orderkey, o_custkey 
        from ${lineitem_table} 
        left join ${orders_table} 
        on ${lineitem_table}.l_orderkey = ${orders_table}.o_orderkey"""
    def mtmv_sql_level2 = """SELECT 
          l_orderkey, 
          l_linenumber, 
          o_orderkey, 
          sum(l_partkey) AS total_revenue, 
          max(o_custkey) AS max_discount 
        FROM ${mv_name}
        GROUP BY l_orderkey, l_linenumber, o_orderkey;"""
    def sql2 = """SELECT 
          t.l_orderkey, 
          t.l_linenumber, 
          t.o_orderkey, 
          sum(t.l_partkey) AS total_revenue, 
          max(t.o_custkey) AS max_discount 
        FROM (select l_shipdate, o_orderdate, l_partkey, l_suppkey, l_orderkey, l_linenumber, o_orderkey, o_custkey 
        from ${lineitem_table} 
        left join ${orders_table} 
        on ${lineitem_table}.l_orderkey = ${orders_table}.o_orderkey) as t
        GROUP BY t.l_orderkey, t.l_linenumber, t.o_orderkey;"""


    def origin_environment = {
        sql "use ${db}"

        sql """
            drop table if exists ${orders_table}
            """

        sql """CREATE TABLE `${orders_table}` (
              `o_orderkey` BIGINT NULL,
              `o_custkey` INT NULL,
              `o_orderstatus` VARCHAR(1) NULL,
              `o_totalprice` DECIMAL(15, 2)  NULL,
              `o_orderpriority` VARCHAR(15) NULL,
              `o_clerk` VARCHAR(15) NULL,
              `o_shippriority` INT NULL,
              `o_comment` VARCHAR(79) NULL,
              `o_orderdate` DATE not NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`o_orderkey`, `o_custkey`)
            COMMENT 'OLAP'
            auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
            DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

        sql """
            drop table if exists ${lineitem_table}
            """
        sql """
            drop table if exists ${lineitem_table_new}
            """

        sql """CREATE TABLE `${lineitem_table}` (
              `l_orderkey` BIGINT NULL,
              `l_linenumber` INT NULL,
              `l_partkey` INT NULL,
              `l_suppkey` INT NULL,
              `l_quantity` DECIMAL(15, 2) NULL,
              `l_extendedprice` DECIMAL(15, 2) NULL,
              `l_discount` DECIMAL(15, 2) NULL,
              `l_tax` DECIMAL(15, 2) NULL,
              `l_returnflag` VARCHAR(1) NULL,
              `l_linestatus` VARCHAR(1) NULL,
              `l_commitdate` DATE NULL,
              `l_receiptdate` DATE NULL,
              `l_shipinstruct` VARCHAR(25) NULL,
              `l_shipmode` VARCHAR(10) NULL,
              `l_comment` VARCHAR(44) NULL,
              `l_shipdate` DATE not NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
            COMMENT 'OLAP'
            auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
            DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

        sql """
            drop table if exists ${partsupp_table}
            """

        sql """CREATE TABLE `${partsupp_table}` (
              `ps_partkey` INT NULL,
              `ps_suppkey` INT NULL,
              `ps_availqty` INT NULL,
              `ps_supplycost` DECIMAL(15, 2) NULL,
              `ps_comment` VARCHAR(199) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`ps_partkey`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

        sql """
            insert into ${orders_table} values 
            (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
            (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
            (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
            (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
            (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
            (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
            (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
            (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
            (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
            (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
            """

        sql """
            insert into ${lineitem_table} values 
            (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
            (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
            (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
            (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
            (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
            (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
            (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
            """

        sql """
            insert into ${partsupp_table} values 
            (1, 1, 1, 99.5, 'yy'),
            (2, 2, 2, 109.2, 'mm'),
            (3, 3, 1, 99.5, 'yy'),
            (3, null, 1, 99.5, 'yy'); 
            """

        sql """analyze table ${orders_table} with sync;"""
        sql """analyze table ${lineitem_table} with sync;"""
        sql """analyze table ${partsupp_table} with sync;"""

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_new};"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
    }

    def waitIndexCreateSucc = {
        sql """use ${db}"""
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        def result
        do {
            result = sql """SHOW INDEX FROM ${lineitem_table};"""
            logger.info("result: " + result.toString())
            if (result.size() == 1) {
                break
            }
            Thread.sleep(1000)
        } while (timeoutTimestamp > System.currentTimeMillis())
        result = sql """SHOW INDEX FROM ${lineitem_table};"""
        assertTrue(result.size() == 1)
        return result.size() == 1
    }

    def waitingColumnTaskFinished = { def dbName, def tableName ->
        Thread.sleep(2000)
        String showTasks = "SHOW ALTER TABLE COLUMN from ${dbName} where TableName='${tableName}' ORDER BY CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(9)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000)
        } while (timeoutTimestamp > System.currentTimeMillis() && (status != 'FINISHED'))
        if (status != "FINISHED") {
            logger.info("status is not success")
            return false
        }
        Assert.assertEquals("FINISHED", status)
        return true
    }



    // table
    def table_alter_rename1 = """alter table ${lineitem_table} rename ${lineitem_table_new}"""
    def table_alter_rename2 = """alter table ${lineitem_table_new} rename ${lineitem_table}"""
    def table_alter_column = """ALTER TABLE ${db}.${lineitem_table} ADD COLUMN new_col INT KEY DEFAULT "0";"""
    def table_alter_add_partition = """ALTER TABLE ${db}.${lineitem_table} ADD PARTITION IF NOT EXISTS p1 VALUES LESS THAN ('2000-01-01')"""
    def table_alter_del_partition = """ALTER TABLE ${db}.${lineitem_table} DROP PARTITION IF EXISTS p1;"""
    def table_alter_rollup = """alter table ${db}.${lineitem_table} add rollup rollup_life_lock(l_partkey)"""
    def table_alter_mv = """create materialized view mv_life_lock as select l_suppkey from ${db}.${lineitem_table};"""
    def table_alter_add_index = """CREATE INDEX IF NOT EXISTS index_life_lock ON ${db}.${lineitem_table} (l_shipdate) USING INVERTED COMMENT 'balabala';"""
    def table_alter_del_index = """DROP INDEX IF EXISTS index_life_lock ON ${db}.${lineitem_table} ;"""
    def table_drop = """drop table ${db}.${lineitem_table}"""
    def table_create = """CREATE TABLE if not exists ${db}.`${lineitem_table}` (
              `l_orderkey` BIGINT NULL,
              `l_linenumber` INT NULL,
              `l_partkey` INT NULL,
              `l_suppkey` INT NULL,
              `l_quantity` DECIMAL(15, 2) NULL,
              `l_extendedprice` DECIMAL(15, 2) NULL,
              `l_discount` DECIMAL(15, 2) NULL,
              `l_tax` DECIMAL(15, 2) NULL,
              `l_returnflag` VARCHAR(1) NULL,
              `l_linestatus` VARCHAR(1) NULL,
              `l_commitdate` DATE NULL,
              `l_receiptdate` DATE NULL,
              `l_shipinstruct` VARCHAR(25) NULL,
              `l_shipmode` VARCHAR(10) NULL,
              `l_comment` VARCHAR(44) NULL,
              `l_shipdate` DATE not NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
            COMMENT 'OLAP'
            auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
            DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    def table_truncate = """truncate table ${db}.${lineitem_table}"""
    def table_insert = """insert into ${db}.${lineitem_table} values 
            (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-30'),
            (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
            (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');"""
    def table_delete = """DELETE FROM ${db}.${lineitem_table} WHERE l_orderkey = 3;"""
    def table_select1 = """select * from ${db}.${lineitem_table};"""
    def table_select2 = """select * from ${db}.${orders_table};"""

    // mtmv
    def mtmv_create1 = """
            CREATE MATERIALIZED VIEW ${db}.${mv_name} 
            BUILD IMMEDIATE REFRESH AUTO ON commit 
            partition by(l_shipdate) 
            DISTRIBUTED BY RANDOM BUCKETS 2 
            PROPERTIES ('replication_num' = '1')  
            AS  
            ${mtmv_sql}
            """
    def mtmv_create2 = """
            CREATE MATERIALIZED VIEW ${db}.${mv_name2} 
            BUILD IMMEDIATE REFRESH AUTO ON commit 
            DISTRIBUTED BY RANDOM BUCKETS 2 
            PROPERTIES ('replication_num' = '1')  
            AS  
            ${mtmv_sql_level2}
            """
    def mtmv_refresh1 = """refresh MATERIALIZED VIEW ${db}.${mv_name} auto;"""
    def mtmv_refresh2 = """refresh MATERIALIZED VIEW ${db}.${mv_name2} auto;"""
    def mtmv_alter_rename1 = """ALTER MATERIALIZED VIEW ${mv_name} rename ${mv_name_new};"""
    def mtmv_alter_rename2 = """ALTER MATERIALIZED VIEW ${mv_name_new} rename ${mv_name};"""
    def mtmv_alter_property = """ALTER MATERIALIZED VIEW ${db}.${mv_name} set("grace_period"="0");"""
    def mtmv_drop = """drop MATERIALIZED VIEW ${db}.${mv_name};"""
    def mtmv_drop1 = """drop MATERIALIZED VIEW if exists ${db}.${mv_name};"""
    def mtmv_drop2 = """drop MATERIALIZED VIEW if exists ${db}.${mv_name2};"""
    def mtmv_pause_job = """PAUSE MATERIALIZED VIEW JOB ON ${db}.${mv_name};"""
    def mtmv_resume_job = """RESUME MATERIALIZED VIEW JOB ON ${db}.${mv_name};"""
    def mtmv_cancel_job = """CANCEL MATERIALIZED VIEW TASK 1 on ${db}.${mv_name};"""
    def mtmv_select1 = """select * from ${db}.${mv_name}"""
    def mtmv_select2 = """select * from ${db}.${mv_name2}"""

    // 给基表更改名称，然后基表创建mtmv
    def judge_table_res = true
    def judge_mtmv_res = true
    def thread_timeout = 5 * 60 * 1000

    def table_alter_col_func = {
        logger.info("table_alter_col_thread2 start")
        int i = 1
        def alter_column_tmp = table_alter_column
        while (judge_mtmv_res && judge_table_res) {
            try {
                sql alter_column_tmp.replaceAll("new_col", "col_${i}")
                if (!waitingColumnTaskFinished(db, lineitem_table)) {
                    judge_table_res = false
                }
            } catch (Exception e) {
                judge_table_res = false
                log.info(e.getMessage())
            }
            i++
        }
    }
    def table_part_func = {
        logger.info("table_part_thread3 start")
        while (judge_mtmv_res && judge_table_res) {
            try {
                sql table_alter_add_partition
                sql table_alter_del_partition
            } catch (Exception e) {
                log.info(e.getMessage())
                judge_table_res = false
            }
        }
    }
    def table_rollup_func = {
        logger.info("table_rollup_thread5 start")
        int i = 1
        def alter_rollup_tmp = table_alter_rollup
        while (judge_mtmv_res && judge_table_res) {
            try {
                sql alter_rollup_tmp.replaceAll("rollup_life_lock", "rollup_life_lock${i}")
                waitingMVTaskFinishedByMvName(db, lineitem_table)
            } catch (Exception e) {
                log.info(e.getMessage())
                judge_table_res = false
            }
            i++
        }
    }
    def table_mv_func = {
        logger.info("table_mv_thread6 start")
        int i = 1
        def alter_mv_tmp = table_alter_mv
        while (judge_mtmv_res && judge_table_res) {
            try {
                sql alter_mv_tmp.replaceAll("mv_life_lock", "mv_life_lock${i}")
                waitingMVTaskFinishedByMvName(db, lineitem_table)
            } catch (Exception e) {
                log.info(e.getMessage())
                judge_table_res = false
            }
            i++
        }
    }
    def table_index_func = {
        logger.info("table_index_thread7 start")
        while (judge_mtmv_res && judge_table_res) {
            try {
                sql table_alter_add_index
                if (!waitIndexCreateSucc()) {
                    judge_table_res = false
                }
                sql table_alter_del_index
            } catch (Exception e) {
                log.info(e.getMessage())
                judge_table_res = false
            }

        }
    }
    def table_data_change_func = {
        logger.info("table_data_change_thread9 start")
        while (judge_mtmv_res && judge_table_res) {
            try {
                sql """set delete_without_partition=true;"""
                sql table_truncate
                sql table_insert
                sql table_delete
            } catch (Exception e) {
                log.info(e.getMessage())
                judge_table_res = false
            }
            sleep(1000)
        }
    }
    def table_select_func = {
        logger.info("table_select_thread10 start")
        while (judge_mtmv_res && judge_table_res) {
            try {
                sql table_select1
                sql table_select2
                sql mtmv_sql
                sql sql2
            } catch (Exception e) {
                log.info(e.getMessage())
                judge_table_res = false
            }
            sleep(1000)
        }
    }


    def sleep_func = {
        sleep(5 * 1000)
    }

    def mtmv_create_func = {
        logger.info("mtmv_create_thread1 start")
        try {
            for (int i = 0; i < 5; i++) {
                sql mtmv_create1
                sql mtmv_create2
                sql mtmv_drop1
                sql mtmv_drop2
                sleep(2 * 1000)
            }
            sleep_func()
        } catch (Exception e) {
            log.info(e.getMessage())
        } finally {
            judge_mtmv_res = false
        }
        logger.info("mtmv_create_thread1 end")
    }
    def mtmv_refresh_func = {
        logger.info("mtmv_refresh_thread2 start")
        try {
            for (int i = 0; i < 5; i++) {
                sql mtmv_refresh1
                sql mtmv_refresh2
                sleep(2 * 1000)
            }
            sleep_func()
        } catch (Exception e) {
            log.info(e.getMessage())
        } finally {
            judge_mtmv_res = false
        }
        logger.info("mtmv_refresh_thread2 end")
    }
    def mtmv_rename_func = {
        logger.info("mtmv_rename_thread3 start")
        try {
            for (int i = 0; i < 5; i++) {
                sql mtmv_alter_rename1
                sql mtmv_alter_rename2
                sleep(2 * 1000)
            }
            sleep_func()
        } catch (Exception e) {
            log.info(e.getMessage())
        } finally {
            judge_mtmv_res = false
        }
        logger.info("mtmv_rename_thread3 end")
    }
    def mtmv_alter_property_func = {
        logger.info("mtmv_alter_property_thread4 start")
        try {
            for (int i = 0; i < 5; i++) {
                sql mtmv_alter_property
                sleep(2 * 1000)
            }
            sleep_func()
        } catch (Exception e) {
            log.info(e.getMessage())
        } finally {
            judge_mtmv_res = false
        }
        logger.info("mtmv_alter_property_thread4 end")
    }
    def mtmv_pause_resume_func = {
        logger.info("mtmv_pause_resume_thread6 start")
        try {
            for (int i = 0; i < 5; i++) {
                sql mtmv_pause_job
                sql mtmv_resume_job
                sleep(2 * 1000)
            }
            sleep_func()
        } catch (Exception e) {
            log.info(e.getMessage())
        } finally {
            judge_mtmv_res = false
        }
        logger.info("mtmv_pause_resume_thread6 end")
    }
    def mtmv_cancel_func = {
        logger.info("mtmv_cancel_thread7 start")
        try {
            for (int i = 0; i < 5; i++) {
                sql mtmv_refresh1
                sql mtmv_cancel_job
                sleep(2 * 1000)
            }
            sleep_func()
        } catch (Exception e) {
            log.info(e.getMessage())
        } finally {
            judge_mtmv_res = false
        }
        logger.info("mtmv_cancel_thread7 end")
    }
    def mtmv_select_func = {
        logger.info("table_select_thread10 start")
        while (judge_mtmv_res) {
            try {
                sql mtmv_select1
                sql mtmv_select2
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("does not exist"))
                if (!e.getMessage().contains("does not exist")) {
                    judge_table_res = false
                }
            }
            sleep(1000)
        }
    }

    def init_environment = {
        judge_table_res = true
        judge_mtmv_res = true
        origin_environment()
        sql mtmv_drop1
        sql mtmv_drop2
        sql mtmv_create1
        sql mtmv_create2
    }

    def threadTimeout = { Thread cur_thread ->
        if (cur_thread.isAlive()) {
            logger.info("thread timeout")
            judge_table_res == false
        }
    }


    logger.info("table alter column + mtmv create")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    def table_alter_col_thread = Thread.start{
        table_alter_col_func()
    }
    def table_select_thread = Thread.start{
        table_select_func()
    }
    def mtmv_create_thread = Thread.start{
        mtmv_create_func()
    }
    def mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_alter_col_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_create_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_alter_col_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_create_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter partition + mtmv create")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    def table_part_thread = Thread.start{
        table_part_func()
    }
    def table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_create_thread = Thread.start{
        mtmv_create_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_part_thread.join(thread_timeout)
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_create_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_part_thread)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_create_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter rollup + mtmv create")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    def table_rollup_thread = Thread.start{
        table_rollup_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_create_thread = Thread.start{
        mtmv_create_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_rollup_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_create_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_rollup_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_create_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter mv + mtmv create")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    def table_mv_thread = Thread.start{
        table_mv_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_create_thread = Thread.start{
        mtmv_create_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_mv_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_create_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_mv_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_create_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter index + mtmv create")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    def table_index_thread = Thread.start{
        table_index_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_create_thread = Thread.start{
        mtmv_create_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_index_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_create_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_index_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_create_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table date change + mtmv create")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_create_thread = Thread.start{
        mtmv_create_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_create_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_create_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


//
    logger.info("table alter column + mtmv refresh")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_alter_col_thread = Thread.start{
        table_alter_col_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    def mtmv_refresh_thread = Thread.start{
        mtmv_refresh_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_alter_col_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_refresh_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_alter_col_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_refresh_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter partition + mtmv refresh")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_part_thread = Thread.start{
        table_part_func()
    }
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_refresh_thread = Thread.start{
        mtmv_refresh_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_part_thread.join(thread_timeout)
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_refresh_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_part_thread)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_refresh_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter rollup + mtmv refresh")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_rollup_thread = Thread.start{
        table_rollup_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_refresh_thread = Thread.start{
        mtmv_refresh_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_rollup_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_refresh_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_rollup_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_refresh_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter mv + mtmv refresh")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_mv_thread = Thread.start{
        table_mv_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_refresh_thread = Thread.start{
        mtmv_refresh_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_mv_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_refresh_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_mv_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_refresh_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter index + mtmv refresh")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_index_thread = Thread.start{
        table_index_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_refresh_thread = Thread.start{
        mtmv_refresh_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_index_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_refresh_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_index_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_refresh_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table date change + mtmv refresh")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_refresh_thread = Thread.start{
        mtmv_refresh_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_refresh_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_refresh_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    //
    logger.info("table alter column + mtmv rename")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_alter_col_thread = Thread.start{
        table_alter_col_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    def mtmv_rename_thread = Thread.start{
        mtmv_rename_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_alter_col_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_rename_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_alter_col_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_rename_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter partition + mtmv rename")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_part_thread = Thread.start{
        table_part_func()
    }
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_rename_thread = Thread.start{
        mtmv_rename_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_part_thread.join(thread_timeout)
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_rename_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_part_thread)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_rename_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter rollup + mtmv rename")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_rollup_thread = Thread.start{
        table_rollup_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_rename_thread = Thread.start{
        mtmv_rename_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_rollup_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_rename_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_rollup_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_rename_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter mv + mtmv rename")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_mv_thread = Thread.start{
        table_mv_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_rename_thread = Thread.start{
        mtmv_rename_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_mv_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_rename_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_mv_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_rename_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter index + mtmv rename")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_index_thread = Thread.start{
        table_index_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_rename_thread = Thread.start{
        mtmv_rename_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_index_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_rename_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_index_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_rename_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table date change + mtmv rename")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_rename_thread = Thread.start{
        mtmv_rename_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_rename_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_rename_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    //
    logger.info("table alter column + mtmv alter property")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_alter_col_thread = Thread.start{
        table_alter_col_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    def mtmv_alter_property_thread = Thread.start{
        mtmv_alter_property_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_alter_col_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_alter_property_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_alter_col_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_alter_property_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter partition + mtmv alter property")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_part_thread = Thread.start{
        table_part_func()
    }
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_alter_property_thread = Thread.start{
        mtmv_alter_property_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_part_thread.join(thread_timeout)
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_alter_property_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_part_thread)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_alter_property_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter rollup + mtmv alter property")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_rollup_thread = Thread.start{
        table_rollup_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_alter_property_thread = Thread.start{
        mtmv_alter_property_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_rollup_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_alter_property_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_rollup_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_alter_property_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter mv + mtmv alter property")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_mv_thread = Thread.start{
        table_mv_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_alter_property_thread = Thread.start{
        mtmv_alter_property_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_mv_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_alter_property_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_mv_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_alter_property_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter index + mtmv alter property")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_index_thread = Thread.start{
        table_index_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_alter_property_thread = Thread.start{
        mtmv_alter_property_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_index_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_alter_property_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_index_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_alter_property_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table date change + mtmv alter property")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_alter_property_thread = Thread.start{
        mtmv_alter_property_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_alter_property_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_alter_property_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    //
    logger.info("table alter column + mtmv pause resume")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_alter_col_thread = Thread.start{
        table_alter_col_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    def mtmv_pause_resume_thread = Thread.start{
        mtmv_pause_resume_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_alter_col_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_pause_resume_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_alter_col_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_pause_resume_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter partition + mtmv pause resume")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_part_thread = Thread.start{
        table_part_func()
    }
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_pause_resume_thread = Thread.start{
        mtmv_pause_resume_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_part_thread.join(thread_timeout)
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_pause_resume_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_part_thread)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_pause_resume_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter rollup + mtmv pause resume")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_rollup_thread = Thread.start{
        table_rollup_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_pause_resume_thread = Thread.start{
        mtmv_pause_resume_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_rollup_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_alter_property_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_rollup_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_pause_resume_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter mv + mtmv pause resume")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_mv_thread = Thread.start{
        table_mv_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_pause_resume_thread = Thread.start{
        mtmv_pause_resume_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_mv_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_pause_resume_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_mv_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_pause_resume_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter index + mtmv pause resume")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_index_thread = Thread.start{
        table_index_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_pause_resume_thread = Thread.start{
        mtmv_pause_resume_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_index_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_pause_resume_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_index_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_pause_resume_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table date change + mtmv pause resume")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_pause_resume_thread = Thread.start{
        mtmv_pause_resume_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_pause_resume_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_pause_resume_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    //
    logger.info("table alter column + mtmv cancel")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_alter_col_thread = Thread.start{
        table_alter_col_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    def mtmv_cancel_thread = Thread.start{
        mtmv_cancel_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_alter_col_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_cancel_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_alter_col_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_cancel_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter partition + mtmv cancel")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_part_thread = Thread.start{
        table_part_func()
    }
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_cancel_thread = Thread.start{
        mtmv_cancel_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_part_thread.join(thread_timeout)
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_cancel_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_part_thread)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_cancel_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter rollup + mtmv cancel")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_rollup_thread = Thread.start{
        table_rollup_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_cancel_thread = Thread.start{
        mtmv_cancel_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_rollup_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_cancel_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_rollup_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_cancel_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

    logger.info("table alter mv + mtmv cancel")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_mv_thread = Thread.start{
        table_mv_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_cancel_thread = Thread.start{
        mtmv_cancel_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_mv_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_cancel_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_mv_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_cancel_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table alter index + mtmv cancel")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_index_thread = Thread.start{
        table_index_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_cancel_thread = Thread.start{
        mtmv_cancel_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_index_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_cancel_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_index_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_cancel_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)


    logger.info("table date change + mtmv cancel")
    init_environment()
    sql mtmv_drop1
    sql mtmv_drop2
    table_data_change_thread = Thread.start{
        table_data_change_func()
    }
    table_select_thread = Thread.start{
        table_select_func()
    }
    mtmv_cancel_thread = Thread.start{
        mtmv_cancel_func()
    }
    mtmv_select_thread = Thread.start {
        mtmv_select_func()
    }
    table_data_change_thread.join(thread_timeout)
    table_select_thread.join(thread_timeout)
    mtmv_cancel_thread.join(thread_timeout)
    mtmv_select_thread.join(thread_timeout)
    threadTimeout(table_data_change_thread)
    threadTimeout(table_select_thread)
    threadTimeout(mtmv_cancel_thread)
    threadTimeout(mtmv_select_thread)
    assertTrue(judge_table_res == true)

}


