import java.nio.file.Files
import java.nio.file.Paths
import java.net.URL
import java.io.File

// 自动统计信息收集功能测试
suite("test_auto_analyze") {

    def clear_stats = {
        logger.info("clear_stats start")

        sql """delete from __internal_schema.column_statistics where cast (id as string) > '0';"""
        sql """delete from __internal_schema.histogram_statistics where cast (id as string) > '0';"""
    }
    clear_stats()

    def init_property = {
        sql """SET enable_nereids_planner=true;"""
        sql """SET enable_fallback_to_original_planner=false;"""
        sql """SET forbid_unknown_col_stats=true;"""

        sql """set global full_auto_analyze_start_time="00:00:00";"""
        sql """set global full_auto_analyze_end_time="23:59:59";"""

        sql """SET query_timeout=180000;"""
    }

    def auto_analyze_db_name = "stats_test"
    def auto_analyze_duplicate_tbl_name = "duplicate_all"

    def drop_db = {
        sql """DROP DATABASE IF EXISTS ${auto_analyze_db_name};"""
    }
    drop_db()

    def create_db = {
        sql """create database ${auto_analyze_db_name};"""
    }
    create_db()
    init_property()

    def auto_analyze_incremental_analyze_test_tbl_name = "t1"
    def auto_analyze_change_schema_test_tbl_name = "t2"
    def auto_analyze_change_tbl_name_1 = "t3"
    def auto_analyze_change_tbl_name_2 = "t4"
    def auto_analyze_interval_in_minutes = 1
    // 增加数据，验证统计信息是否自动更新
    def test_incremental_analyze = {
        sql """use stats_test;"""
        sql """DROP TABLE IF EXISTS ${auto_analyze_incremental_analyze_test_tbl_name}"""
        sql """CREATE TABLE ${auto_analyze_incremental_analyze_test_tbl_name} (
            col1 varchar(11451) not null, col2 int not null, col3 int not null)
            DUPLICATE KEY(col1)
            DISTRIBUTED BY HASH(col1)
            BUCKETS 3
            PROPERTIES(
                "replication_num"="1"
            );
        """
        sql """insert into t1 values(1, 2, 3);"""
        sql """insert into t1 values(4, 5, 6);"""
        sql """insert into t1 values(7, 1, 9);"""
        sql """ANALYZE TABLE ${auto_analyze_incremental_analyze_test_tbl_name} WITH SYNC;"""
        sql """insert into t1 values(3, 8, 2);"""
        sql """insert into t1 values(5, 2, 1);"""
        sql """insert into t1 values(41, 2, 3);"""
        sql """insert into t1 values(54, 5, 6);"""
        def max_wait_time_in_minutes = 45 * 60
        def waiting_time = 0
        while (waiting_time < max_wait_time_in_minutes) {
            def sleep_time_num = auto_analyze_interval_in_minutes * 2 * 60
            sleep(sleep_time_num * 1000)
            waiting_time += sleep_time_num
            def r = sql """SHOW COLUMN STATS t1(col1);"""
            if ((r[0][1] as float as int) == 7) {
                logger.info("Add some data rows is work")
                break
            }
        }
        if (waiting_time >= max_wait_time_in_minutes) {
            logger.info("auto analyze failed due to time out")
            return false
        }
        return true
    }
    assertTrue(test_incremental_analyze())


    // 增加一列，查看表的统计数据是否符合预期
    def change_column = {
        sql """use stats_test;"""
        sql """DROP TABLE IF EXISTS ${auto_analyze_change_schema_test_tbl_name}"""
        sql """CREATE TABLE ${auto_analyze_change_schema_test_tbl_name} (
            col4 varchar(11451) not null, col5 int not null, col6 int not null)
            DUPLICATE KEY(col4)
            DISTRIBUTED BY HASH(col4)
            BUCKETS 3
            PROPERTIES(
                "replication_num"="1"
            );
        """
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(1, 2, 3);"""
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(4, 5, 6);"""
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(7, 1, 9);"""
        sql """alter table ${auto_analyze_change_schema_test_tbl_name} add column col7 int not null default '0' after col6;"""
        // alter table cost_time_table add column analyze_end_time datetime after  load_end_time;
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(54, 5, 6, 11);"""
        def max_sleep_time = 12 * 60
        def cur_sleep_time = 0
        def one_sleep_time = 1
        def time_out_flag = false
        while (true) {
            sleep(one_sleep_time * 60 * 1000)
            cur_sleep_time += one_sleep_time
            def res = sql """show column stats ${auto_analyze_change_schema_test_tbl_name};"""
            if (res.size == 4 && res[0][1] as float as int == 4) { // 统计信息有四列，且有四行数据
                break
            }
            if (cur_sleep_time > max_sleep_time) {
                time_out_flag = true
                break
            }
        }
        return !time_out_flag
    }
    assertTrue(change_column())
    logger.info("Add one column is normal.")

    // 交换两张表，验证统计信息是否可以正确交换
    def change_2_table = {
        sql """use stats_test;"""
        sql """DROP TABLE IF EXISTS ${auto_analyze_change_tbl_name_1}"""
        sql """CREATE TABLE ${auto_analyze_change_tbl_name_1} (
               col1 varchar(11451) not null, col2 int not null, col3 int not null)
               DUPLICATE KEY(col1)
               DISTRIBUTED BY HASH(col1)
               BUCKETS 3
               PROPERTIES(
                    "replication_num"="1"
               );"""
        sql """insert into ${auto_analyze_change_tbl_name_1} values(1, 2, 3);"""
        sql """insert into ${auto_analyze_change_tbl_name_1} values(4, 5, 6);"""
        sql """insert into ${auto_analyze_change_tbl_name_1} values(7, 1, 9);"""

        sql """DROP TABLE IF EXISTS ${auto_analyze_change_tbl_name_2}"""
        sql """CREATE TABLE ${auto_analyze_change_tbl_name_2} (
               col4 varchar(11451) not null, col5 int not null, col6 int not null)
               DUPLICATE KEY(col4)
               DISTRIBUTED BY HASH(col4)
               BUCKETS 3
               PROPERTIES(
                    "replication_num"="1"
               );"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(3, 8, 2);"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(5, 2, 1);"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(41, 2, 3);"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(54, 5, 6);"""

        while (true) {
            sleep(1 * 60 * 1000)
            def res = sql """show column stats ${auto_analyze_change_tbl_name_1};"""
            if (res[0][1] as float as int == 3) break;
        }
        while (true) {
            def res = sql """show column stats ${auto_analyze_change_tbl_name_2};"""
            if (res[0][1] as float as int == 4) break;
            sleep(1 * 60 * 1000)
        }
        sql """ALTER TABLE ${auto_analyze_change_tbl_name_1} REPLACE WITH TABLE ${auto_analyze_change_tbl_name_2} PROPERTIES('swap' = 'true');"""
        def res1 = sql """show column stats ${auto_analyze_change_tbl_name_1};"""
        assertTrue(res1[0][1] as float as int == 4)

        def res2 = sql """show column stats ${auto_analyze_change_tbl_name_2};"""
        assertTrue(res2[0][1] as float as int == 3)
        logger.info("change two table name is normal.")
    }
    change_2_table()



    sql """set global full_auto_analyze_start_time="00:00:00";"""
    sql """set global full_auto_analyze_end_time="02:00:00";"""








}
