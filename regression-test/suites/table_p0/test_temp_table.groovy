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

import org.junit.Assert;

suite("test_temp_table") {
    sql """
        DROP TABLE IF EXISTS `t_test_table_with_data`
    """
    sql """
        CREATE TABLE `t_test_table_with_data` (
            `id` int,
            `add_date` date,
            `name` varchar(32),
        ) ENGINE=OLAP
        UNIQUE KEY(`id`,`add_date`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`add_date`)
        (
        PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_3000 VALUES [('2017-03-01'), ('2017-04-01'))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
        """

    sql """
        insert into t_test_table_with_data values (1,"2017-01-15","Alice"),(2,"2017-02-12","Bob"),(3,"2017-03-20","Carl");
        """

    sql """
        CREATE TABLE IF NOT EXISTS `t_test_temp_table1` (
            `id` int,
            `add_date` date,
            `name` varchar(32),
        ) ENGINE=OLAP
        UNIQUE KEY(`id`,`add_date`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`add_date`)
        (
        PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_3000 VALUES [('2017-03-01'), ('2017-04-01'))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
        """

    // temporary table have same name with common olap table
    sql """
        CREATE TEMPORARY TABLE `t_test_temp_table1` (
            `id` int,
            `add_date` date,
            `name` varchar(32),
        ) ENGINE=OLAP
        UNIQUE KEY(`id`,`add_date`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`add_date`)
        (
        PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_3000 VALUES [('2017-03-01'), ('2017-04-01'))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'storage_medium' = 'hdd',
            'storage_cooldown_time' = '2040-11-20 00:00:00',
            'bloom_filter_columns' = 'add_date',
            'enable_unique_key_merge_on_write' = 'true',
            'function_column.sequence_col' = 'add_date'
        );
        """

    // create table as
    sql """ 
        create temporary table t_test_temp_table2 PROPERTIES (
            'replication_num' = '1', 'light_schema_change' = 'true', 'bloom_filter_columns' = 'add_date'
        ) 
        as select * from t_test_temp_table1;
        """

    def show_tables = sql "show tables"
    def hasTempTable = false
    for(int i = 0; i < show_tables.size(); i++) {
        if (show_tables[i][0].equals("t_test_temp_table2")) {
            hasTempTable = true;
        }
    }
    assertFalse(hasTempTable)

    def show_data = sql "show data"
    def containTempTable = false
    for(int i = 0; i < show_data.size(); i++) {
        if (show_data[i][0].equals("t_test_temp_table2")) {
            containTempTable = true;
        }
    }
    assertTrue(containTempTable)

    def desc_result = sql "desc t_test_temp_table2"
    assertEquals(desc_result.size(), 3)

    def show_partition_result = sql "show partitions from t_test_temp_table2"
    assertEquals(show_partition_result.size(), 1)

    def show_column_result = sql "show full columns from t_test_temp_table2"
    assertEquals(show_column_result.size(), 3)

    def show_tablets_result = sql "show tablets from t_test_temp_table1"
    assertEquals(show_tablets_result.size(), 3)

    sql "show column stats t_test_temp_table2;"

    sql "show data skew from t_test_temp_table2"

    def show_result = sql "show create table t_test_temp_table1"
    assertEquals(show_result[0][0], "t_test_temp_table1")
    assertTrue(show_result[0][1].contains("CREATE TEMPORARY TABLE"))

    sql """
        insert into t_test_temp_table1 select * from t_test_table_with_data;
        """
    sql """
        insert into t_test_temp_table2 select * from t_test_table_with_data;
        """

    // must be in front of update, update is a kind of insert
    def show_insert = sql "show last insert"
    containTempTable = false
    for(int i = 0; i < show_insert.size(); i++) {
        if (show_insert[i][3].equals("t_test_temp_table2")) {
            containTempTable = true;
        }
    }
    assertTrue(containTempTable)

    sql """
        update t_test_temp_table1 set name='Blair' where id=2;
        """

    sql """
        delete from t_test_temp_table2 where id=3;
        """

    def select_result1 = sql "select * from t_test_temp_table2"
    assertEquals(select_result1.size(), 2)

    def select_result2 = sql "select t1.* from t_test_temp_table1 t1 join t_test_temp_table2 t2 on t1.id = t2.id and t1.name = t2.name"
    assertEquals(select_result2.size(), 1)
    assertEquals(select_result2[0][2], "Alice")

    def show_delete = sql "show delete"
    containTempTable = false
    for(int i = 0; i < show_delete.size(); i++) {
        if (show_delete[i][0].equals("t_test_temp_table2")) {
            containTempTable = true;
        }
    }
    assertTrue(containTempTable)

    def show_table_status = sql "show table status"
    containTempTable = false
    for(int i = 0; i < show_table_status.size(); i++) {
        if (show_table_status[i][0].equals("t_test_temp_table2")) {
            containTempTable = true;
        }
    }
    assertTrue(containTempTable)

    //export
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """/tmp/test_export_${uuid}"""

    try {
        sql """
            EXPORT TABLE t_test_temp_table2 TO "file://${outFilePath}"
            PROPERTIES (
                    "columns" = "id,name,add_date",
                    "format" = "parquet"
            )
        """
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("do not support export"), ex.getMessage())
    }

    //outfile
    sql """
        select * from t_test_temp_table2
        into outfile "file://${outFilePath}"
    """


    // temporary table with dynamic partitions and colocate group
    sql """
        CREATE TEMPORARY TABLE temp_table_with_dyncmic_partition
        (
            k1 DATE
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default: 1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-7",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "10",
            "colocate_with" = "colocate_group1"
        );
        """
    def select_partitions1 = sql "show partitions from temp_table_with_dyncmic_partition"
    assertEquals(select_partitions1.size(), 4)

    try {
        sql "CREATE MATERIALIZED VIEW mv_mtmv1 as select k1 from temp_table_with_dyncmic_partition"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("table not found"), ex.getMessage())
    }
    def show_create_mv1 = sql "show create materialized view mv_mtmv1 on temp_table_with_dyncmic_partition"
    assertEquals(show_create_mv1.size(), 0)

    // create another session and check temp table related function in it
    def result2 = connect('root') {
        sql "use regression_test_table_p0"
        // cannot use temp table created by another session
        def show_result3 = sql "show create table t_test_temp_table1"
        assertEquals(show_result3.size(), 1)
        assertFalse(show_result3[0][1].contains("CREATE TEMPORARY TABLE"))

        def select_result3 = sql "select * from t_test_temp_table1"
        assertEquals(select_result3.size(), 0)

        // can create a temp table with anther temp table which is created by another session in the same db
        sql """
        create temporary table t_test_temp_table1 PROPERTIES ('replication_num' = '1') as
        select *
        from t_test_table_with_data;
        """

        // temp table with same name in another db is legal
        sql """create database if not exists regression_test_temp_table_db2"""
        sql """
        create temporary table regression_test_temp_table_db2.t_test_temp_table1 PROPERTIES ('replication_num' = '1', 'compression' = 'ZSTD') as
        select *
        from t_test_table_with_data;
        """

        // only show delete info in current session
        def show_delete2 = sql "show delete"
        containTempTable = false
        for(int i = 0; i < show_delete2.size(); i++) {
            if (show_delete2[i][0].equals("t_test_temp_table2")) {
                containTempTable = true;
            }
        }
        assertFalse(containTempTable)

        // only show table status info in current session
        def show_status = sql "show table status"
        containTempTable = false
        for(int i = 0; i < show_status.size(); i++) {
            if (show_status[i][0].equals("t_test_temp_table2")) {
                containTempTable = true;
            }
        }
        assertFalse(containTempTable)
    }

    // temp tables created by a session will be deleted when the session exit
    try {
        sql """show create table regression_test_temp_table_db2.t_test_temp_table1"""
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Unknown table"), ex.getMessage())
    }

    sql """
        drop table t_test_temp_table1;
        """
    def show_result2 = sql "show create table t_test_temp_table1"
    assertEquals(show_result2.size(), 1)
    assertFalse(show_result2[0][1].contains("CREATE TEMPORARY TABLE"))

    // analyze
    sql """analyze table t_test_temp_table2 with sample percent 10"""
    sleep(6000)
    def show_analyze_result1 = sql "show column stats t_test_temp_table2"
    assertEquals(show_analyze_result1.size(), 3)

    def show_dynamic_partitions = sql "show dynamic partition tables"
    containTempTable = false
    for(int i = 0; i < show_dynamic_partitions.size(); i++) {
        if (show_dynamic_partitions[i][0].equals("temp_table_with_dyncmic_partition")) {
            containTempTable = true;
        }
    }
    assertTrue(containTempTable)

    // truncate
    sql "truncate table t_test_temp_table2"
    select_result3 = sql "select * from t_test_temp_table2"
    assertEquals(select_result3.size(), 0)

    // alter
    try {
        sql "alter table t_test_temp_table2 rename t_test_temp_table2_new"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support alter"), ex.getMessage())
    }
    try {
        sql "alter table t_test_temp_table2 add column new_col int"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support alter"), ex.getMessage())
    }
    try {
        sql "CREATE INDEX bitmap_index_1 ON t_test_temp_table2 (name) USING BITMAP COMMENT 'bitmap_name';"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support alter"), ex.getMessage())
    }
    def show_index_2 = sql "show index from t_test_temp_table2"
    assertEquals(show_index_2.size(), 0)

    sql """drop table temp_table_with_dyncmic_partition"""
    def show_recycle_result1 = sql "show catalog recycle bin"
    containTempTable = false
    for(int i = 0; i < show_recycle_result1.size(); i++) {
        if (show_recycle_result1[i][1].equals("temp_table_with_dyncmic_partition")) {
            containTempTable = true;
        }
    }
    assertFalse(containTempTable)

    // clean
    sql """drop table t_test_temp_table1"""
    sql """drop table if exists t_test_table_with_data"""
    sql """drop database regression_test_temp_table_db2"""
}
