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

suite('test_temp_table', 'p0') {
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
        CREATE TABLE IF NOT EXISTS `t_test_table_no_partition` (
          `id` INT,
          `name` varchar(32),
          gender varchar(512)
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into t_test_table_with_data values (1,"2017-01-15","Alice"),(2,"2017-02-12","Bob"),(3,"2017-03-20","Carl");
        """

    sql """
        CREATE TABLE IF NOT EXISTS `t_test_temp_table1` (
            `id` int,
            `add_date` date,
            `name` varchar(32)
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
            'function_column.sequence_col' = 'add_date',
            'disable_auto_compaction' = 'true',
            'compaction_policy' = 'size_based',
            'group_commit_interval_ms' = '2000',
            'enable_mow_light_delete' = 'true'
        );
        """

    // create table as
    sql """ 
        create temporary table t_test_temp_table2 PROPERTIES (
            'replication_num' = '1', 'light_schema_change' = 'true', 'bloom_filter_columns' = 'add_date'
        ) 
        as select * from t_test_temp_table1;
        """

    // create table like
    sql "create temporary table t_test_temp_table3 like t_test_table_no_partition"

    def show_tables = sql "show tables"
    def hasTempTable = false
    for(int i = 0; i < show_tables.size(); i++) {
        if (show_tables[i][0].contains("t_test_temp_table2")) {
            hasTempTable = true;
        }
    }
    assertFalse(hasTempTable)

    // will create a normal olap table, not temporary table, even if source table is temporary
    sql "drop table if exists t_test_table3_0"
    sql "create table t_test_table3_0 like t_test_temp_table3"
    show_tables = sql "show tables"
    def hasTable = false
    for(int i = 0; i < show_tables.size(); i++) {
        if (show_tables[i][0].contains("t_test_table3_0")) {
            hasTable = true;
        }
    }
    assertTrue(hasTable)

    // transaction
    sql "begin"
    sql "insert into t_test_temp_table2 values (4,\"2018-06-15\",\"David\"),(5,\"2018-07-12\",\"Elliott\")"
    sql "rollback"
    def select_result11 = sql "select * from t_test_temp_table2"
    assertEquals(select_result11.size(), 0)

    sql "begin"
    sql "insert into t_test_temp_table2 values (4,\"2018-06-15\",\"David\"),(5,\"2018-07-12\",\"Elliott\")"
    sql "commit"
    select_result11 = sql "select * from t_test_temp_table2"
    assertEquals(select_result11.size(), 2)

    sql "begin"
    sql "insert into t_test_temp_table2 select * from t_test_table_with_data"
    sql "insert into t_test_temp_table3 select * from t_test_table_with_data"
    sql "rollback"
    select_result11 = sql "select * from t_test_temp_table2"
    assertEquals(select_result11.size(), 2)
    select_result11 = sql "select * from t_test_temp_table3"
    assertEquals(select_result11.size(), 0)

    sql "begin"
    sql "insert into t_test_temp_table2 select * from t_test_table_with_data"
    sql "insert into t_test_temp_table3 select * from t_test_table_with_data"
    sql "commit"
    select_result11 = sql "select * from t_test_temp_table2"
    assertEquals(select_result11.size(), 5)
    select_result11 = sql "select * from t_test_temp_table3"
    assertEquals(select_result11.size(), 3)

    sql "insert overwrite table t_test_temp_table2 select * from t_test_table_with_data"
    select_result11 = sql "select * from t_test_temp_table2"
    assertEquals(select_result11.size(), 3)

    // truncate
    sql "truncate table t_test_temp_table2"
    def select_result3 = sql "select * from t_test_temp_table2"
    assertEquals(select_result3.size(), 0)
    sql "truncate table t_test_temp_table1"
    select_result3 = sql "select * from t_test_temp_table1"
    assertEquals(select_result3.size(), 0)
    sql "truncate table t_test_temp_table3"
    select_result3 = sql "select * from t_test_temp_table3"
    assertEquals(select_result3.size(), 0)

    try {
        sql """
            CREATE TEMPORARY TABLE `t_test_temp_table_with_rollup` (
              `id` INT,
              `name` varchar(32),
              `gender` boolean
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            ROLLUP (r1(name, id))
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Do not support temporary table with rollup"), ex.getMessage())
    }

    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "snst_" + UUID.randomUUID().toString().replace("-", "")
    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    try {
        sql """
            BACKUP SNAPSHOT regression_test_temp_table_p0.${snapshotName}
            to ${repoName}
            on (t_test_temp_table2)
        """
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        log.info(ex.getMessage())
    }

    if (!isCloudMode()) {
        //a job backup multiple tables will submit successfully, and temp table will be filtered
        snapshotName = "snst_" + UUID.randomUUID().toString().replace("-", "")
        sql """
            BACKUP SNAPSHOT regression_test_temp_table_p0.${snapshotName}
            to ${repoName}
        """
        select_result3 = sql "show backup where SnapshotName = '${snapshotName}'"
        assertFalse(select_result3[0][4].contains("t_test_temp_table2"))
    }

    def show_data = sql "show data"
    def containTempTable = false
    for(int i = 0; i < show_data.size(); i++) {
        if (show_data[i][0].contains("_#TEMP#_t_test_temp_table2")) {
            containTempTable = true;
        }
    }
    assertTrue(containTempTable)

    def desc_result = sql "desc t_test_temp_table2"
    assertEquals(desc_result.size(), 3)

    desc_result = sql "show full fields from t_test_temp_table3"
    assertEquals(desc_result.size(), 3)

    desc_result = sql "show full columns from t_test_temp_table3"
    assertEquals(desc_result.size(), 3)

    show_data = sql "show partitions from t_test_temp_table3"
    assertEquals(show_data.size(), 1)
    assertEquals(show_data[0][1], "t_test_temp_table3")

    def partition_id = show_data[0][0]
    show_data = sql "show partition ${partition_id}"
    assertEquals(show_data.size(), 1)
    assertEquals(show_data[0][1], "t_test_temp_table3")
    assertEquals(show_data[0][2], "t_test_temp_table3")

    def table_id = show_data[0][4]
    show_data = sql "show table ${table_id}"
    assertEquals(show_data.size(), 1)
    assertEquals(show_data[0][1], "t_test_temp_table3")

    desc_result = sql "show query stats from t_test_temp_table2"
    assertEquals(desc_result.size(), 3)

    def show_column_result = sql "show full columns from t_test_temp_table2"
    assertEquals(show_column_result.size(), 3)

    def show_tablets_result = sql "show tablets from t_test_temp_table1"
    assertEquals(show_tablets_result.size(), 3)
    def tablet_id = show_tablets_result[0][0]
    // admin user will see temporary table's internal name
    show_tablets_result = sql "show tablet ${tablet_id}"
    assertTrue(show_tablets_result[0][1].contains("_#TEMP#_t_test_temp_table1"))
    show_tablets_result = sql "show tablets belong ${tablet_id}"
    assertTrue(show_tablets_result[0][1].contains("_#TEMP#_t_test_temp_table1"))

    sql "show column stats t_test_temp_table2;"

    show_tablets_result = sql "SHOW TABLE STATS t_test_temp_table2"
    assertFalse(show_tablets_result[0][4].contains("_#TEMP#"))

    sql "show data skew from t_test_temp_table2"

    try {
        sql """
            CREATE TEMPORARY TABLE `t_test_temp_table_with_binlog` (
              `id` INT,
              `name` varchar(32),
              `gender` boolean
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
            );
        """
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Cannot create temporary table with binlog enable"), ex.getMessage())
    }
    try {
        sql """
            CREATE TEMPORARY TABLE `t_test_temp_table_with_binlog2`
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true"
            ) as select * from t_test_table_with_data;
        """
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Cannot create temporary table with binlog enable"), ex.getMessage())
    }

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

    // insert into values to partitioned temporary table
    sql """
        INSERT INTO t_test_temp_table1 values (4,"2017-03-15","David"),(5,"2017-03-21","Elliott");
    """
    select_result1 = sql "select * from t_test_temp_table1"
    assertEquals(select_result1.size(), 5)

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
        assertTrue(ex.getMessage().contains("do not support create materialized view on temporary table"), ex.getMessage())
    }
    def show_create_mv1 = sql "show create materialized view mv_mtmv1 on temp_table_with_dyncmic_partition"
    assertEquals(show_create_mv1.size(), 0)

    try {
        sql """
            CREATE MATERIALIZED VIEW mtmv_2 REFRESH COMPLETE ON SCHEDULE EVERY 100 hour
            PROPERTIES ('replication_num' = '1') 
            AS SELECT * FROM temp_table_with_dyncmic_partition;
            """

        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("do not support create materialized view on temporary table"), ex.getMessage())
    }

    // create another session and check temp table related function in it
    connect('root') {
        sql "use regression_test_temp_table_p0"
        // cannot use temp table created by another session
        def show_result3 = sql "show create table t_test_temp_table1"
        assertEquals(show_result3.size(), 1)
        assertFalse(show_result3[0][1].contains("CREATE TEMPORARY TABLE"))

        select_result3 = sql "select * from t_test_temp_table1"
        assertEquals(select_result3.size(), 0)

        // can create a temp table which have same name with other temp table which is created in another session in the same db
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

        // admin user see tablet of temporary table which created by other session
        def show_tablets_result2 = sql "show tablet ${tablet_id}"
        assertTrue(show_tablets_result2[0][1].contains("_#TEMP#_t_test_temp_table1"))
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
    sleep(10000)
    show_result2 = sql "show analyze t_test_temp_table2"
    assertEquals(show_result2.size(), 1)
    assertTrue(show_result2[0][3].equals("t_test_temp_table2"))
    assertFalse(show_result2[0][4].contains("_#TEMP#_"))
    sql "show column stats t_test_temp_table2"

    def show_dynamic_partitions = sql "show dynamic partition tables"
    containTempTable = false
    for(int i = 0; i < show_dynamic_partitions.size(); i++) {
        if (show_dynamic_partitions[i][0].equals("temp_table_with_dyncmic_partition")) {
            containTempTable = true;
        }
    }
    assertTrue(containTempTable)

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
        sql "alter table t_test_temp_table2 set (\"in_memory\" = \"false\")"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support alter"), ex.getMessage())
    }

    // alter table replace
    try {
        sql "alter table t_test_temp_table2 replace with table t_test_table_with_data"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support alter"), ex.getMessage())
    }
    try {
        sql "alter table t_test_table_with_data replace with table t_test_temp_table2"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support replace"), ex.getMessage())
    }

    try {
        sql "alter table t_test_temp_table2 replace with table t_test_table_with_data properties ('swap' = 'true')"
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
    try {
        sql "CREATE INDEX bitmap_index_2 ON t_test_temp_table2 (name) USING INVERTED;"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support alter"), ex.getMessage())
    }
    try {
        sql "ALTER TABLE t_test_temp_table2 ADD INDEX bitmap_index_3 (name) USING INVERTED;"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("detailMessage = Do not support alter"), ex.getMessage())
    }
    def show_index_2 = sql "show index from t_test_temp_table2"
    assertEquals(show_index_2.size(), 0)

    // do not support stream load for temporary table
    streamLoad {
        table "t_test_temp_table3"

        set 'column_separator', ','
        file 'temp_table_data.csv'

        time 10000
        check { result, exception, startTime, endTime ->
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.NumberLoadedRows == 0 && json.LoadBytes == 0)
        }
    }
    sql "sync"
    select_result3 = sql "select * from t_test_temp_table3"
    assertEquals(select_result3.size(), 0)

    sql """drop table temp_table_with_dyncmic_partition"""
    def show_recycle_result1 = sql "show catalog recycle bin"
    containTempTable = false
    for(int i = 0; i < show_recycle_result1.size(); i++) {
        if (show_recycle_result1[i][1].equals("temp_table_with_dyncmic_partition")) {
            containTempTable = true;
        }
    }
    assertFalse(containTempTable)

    // test using a user without admin privilege
    sql """
        DROP USER IF EXISTS temp_table_test_user;
        CREATE USER IF NOT EXISTS 'temp_table_test_user'@'%' IDENTIFIED BY '123456';
        GRANT CREATE_PRIV ON internal.*.* TO 'temp_table_test_user'@'%';
        GRANT SELECT_PRIV ON internal.regression_test_temp_table_p0.* TO 'temp_table_test_user'@'%';
        """
    connect('temp_table_test_user', '123456') {
        sql "use regression_test_temp_table_p0"

        // common user cannot see temporary tables created in other sessions
        def show_result1 = sql "show data"
        containTempTable = false
        for(int i = 0; i < show_result1.size(); i++) {
            if (show_result1[i][0].contains("t_test_temp_table3")) {
                containTempTable = true;
            }
        }
        assertFalse(containTempTable)

        sql "create temporary table t_test_temp_table4 like t_test_table_with_data"

        show_result1 = sql "show data"
        containTempTable = false
        for(int i = 0; i < show_result1.size(); i++) {
            if (show_result1[i][0].equals("t_test_temp_table4")) {
                containTempTable = true;
            }
        }
        assertTrue(containTempTable)
    }

    sql "drop database if exists regression_test_temp_table_p0_2"
    sql "create database regression_test_temp_table_p0_2"
    sql "ALTER DATABASE regression_test_temp_table_p0_2 SET properties (\"binlog.enable\" = \"true\");"
    sql """
        CREATE TEMPORARY TABLE IF NOT EXISTS `t_test_temp_table6` (
            `id` int,
            `add_date` date,
            `name` varchar(32)
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        'replication_num' = '1',
        'binlog.enable' = 'false'
        );
    """

    try {
        sql """
            CREATE TEMPORARY TABLE IF NOT EXISTS `t_test_temp_table7` (
                `id` int,
                `add_date` date,
                `name` varchar(32)
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            'replication_num' = '1',
            'binlog.enable' = 'true'
            );
        """
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Cannot create temporary table with binlog enable"), ex.getMessage())
    }

    // clean
    sql "use regression_test_temp_table_p0"
    sql "drop table t_test_temp_table1"
    sql "drop table t_test_table3_0"
    sql "drop table t_test_table_no_partition"
    sql "DROP USER IF EXISTS temp_table_test_user"
    sql """drop table if exists t_test_table_with_data"""
    sql """drop database if exists regression_test_temp_table_db2"""
}
