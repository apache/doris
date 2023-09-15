import java.nio.file.Files
import java.nio.file.Paths
import java.net.URL
import java.io.File

// auto analyze test case
suite("test_auto_analyze") {

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

    // Verify that statistics are updated normally
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
        sql """drop stats ${auto_analyze_incremental_analyze_test_tbl_name};"""
        sql """insert into t1 values(1, 2, 3);"""
        sql """insert into t1 values(4, 5, 6);"""
        sql """insert into t1 values(7, 1, 9);"""
        sql """ANALYZE TABLE ${auto_analyze_incremental_analyze_test_tbl_name} WITH SYNC;"""
        sql """insert into t1 values(3, 8, 2);"""
        sql """insert into t1 values(5, 2, 1);"""
        sql """insert into t1 values(41, 2, 3);"""
        sql """insert into t1 values(54, 5, 6);"""
        def max_wait_time_in_minutes = 20 * 60
        def waiting_time = 0
        while (waiting_time < max_wait_time_in_minutes) {
            def sleep_time_num = auto_analyze_interval_in_minutes * 60
            sleep(sleep_time_num * 1000)
            waiting_time += sleep_time_num
            // after sleeping, check analyze finished or failed
            def analyze_res = sql """show auto analyze ${auto_analyze_incremental_analyze_test_tbl_name};"""
            if (analyze_res != [] && analyze_res[0][9] == "FAILED") {
                return false
            }
            // check analyze result is accuracy
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

    // add one column, verify that the statistics of the table are updated as expected
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
        sql """drop stats ${auto_analyze_change_schema_test_tbl_name};"""
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(1, 2, 3);"""
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(4, 5, 6);"""
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(7, 1, 9);"""
        sql """alter table ${auto_analyze_change_schema_test_tbl_name} add column col7 int not null default '0' after col6;"""
        sql """insert into ${auto_analyze_change_schema_test_tbl_name} values(54, 5, 6, 11);"""
        def max_wait_time_in_minutes = 20 * 60
        def waiting_time = 0
        while (waiting_time < max_wait_time_in_minutes) {
            def sleep_time_num = auto_analyze_interval_in_minutes * 60
            sleep(sleep_time_num * 1000)
            waiting_time += sleep_time_num

            // after sleeping, check analyze finished or failed
            def analyze_res = sql """show auto analyze ${auto_analyze_change_schema_test_tbl_name};"""
            if (analyze_res != [] && analyze_res[0][9] == "FAILED") {
                logger.info("analyze failed")
                break
            }
            // check analyze result is accuracy
            def res = sql """show column stats ${auto_analyze_change_schema_test_tbl_name};"""
            if (res.size == 4 && res[0][1] as float as int == 4) { // stats 4 cols, and 4 rows
                break
            }
        }
        if (waiting_time >= max_wait_time_in_minutes) {
            logger.info("auto analyze failed due to time out")
            return false
        }
        return true
    }
    assertTrue(change_column())
    logger.info("Add one column is normal.")

    // Exchange two tables to verify that statistics can be exchanged correctly
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
        sql """drop stats ${auto_analyze_change_tbl_name_1};"""
        sql """insert into ${auto_analyze_change_tbl_name_1} values(1, 2, 3);"""
        sql """insert into ${auto_analyze_change_tbl_name_1} values(4, 5, 6);"""
        sql """insert into ${auto_analyze_change_tbl_name_1} values(7, 1, 9);"""
        sql """analyze table ${auto_analyze_change_tbl_name_1} with sync;"""
        def res3 = sql """show column stats ${auto_analyze_change_tbl_name_1};"""
        assertTrue(res3[0][1] as float as int == 3)

        sql """DROP TABLE IF EXISTS ${auto_analyze_change_tbl_name_2}"""
        sql """CREATE TABLE ${auto_analyze_change_tbl_name_2} (
               col4 varchar(11451) not null, col5 int not null, col6 int not null)
               DUPLICATE KEY(col4)
               DISTRIBUTED BY HASH(col4)
               BUCKETS 3
               PROPERTIES(
                    "replication_num"="1"
               );"""
        sql """drop stats ${auto_analyze_change_tbl_name_2}"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(3, 8, 2);"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(5, 2, 1);"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(41, 2, 3);"""
        sql """insert into ${auto_analyze_change_tbl_name_2} values(54, 5, 6);"""
        sql """analyze table ${auto_analyze_change_tbl_name_2} with sync;"""
        def res4 = sql """show column stats ${auto_analyze_change_tbl_name_2};"""
        assertTrue(res4[0][1] as float as int == 4)

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
