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

    sql """
        CREATE TEMP TABLE `t_test_temp_table1` (
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
        create temporary table t_test_temp_table2 PROPERTIES ('replication_num' = '1') as
        select *
        from t_test_temp_table1;
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

    def show_result = sql "show create table t_test_temp_table1"
    assertEquals(show_result[0][0], "t_test_temp_table1")
    assertTrue(show_result[0][1].contains("CREATE TEMPORARY TABLE"))

    sql """
        insert into t_test_temp_table1 select * from t_test_table_with_data;
        """
    sql """
        insert into t_test_temp_table2 select * from t_test_table_with_data;
        """

    sql """
        update t_test_temp_table1 set name='Blair' where id=2;
        """

    sql """
        delete from t_test_temp_table1 where id=3;
        """

    sql """
        alter table t_test_temp_table1 add column age int;
        """

    def select_result1 = sql "select * from t_test_temp_table1"
    assertEquals(select_result1.size(), 2)

    def select_result2 = sql "select t1.* from t_test_temp_table1 t1 join t_test_temp_table2 t2 on t1.id = t2.id and t1.name = t2.name"
    assertEquals(select_result2.size(), 1)
    assertEquals(select_result2[0][2], "Alice")


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
        create temp table regression_test_temp_table_db2.t_test_temp_table1 PROPERTIES ('replication_num' = '1') as
        select *
        from t_test_table_with_data;
        """
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

    sql """drop table t_test_temp_table1"""
    sql """drop table if exists t_test_table_with_data"""
    sql """drop database regression_test_temp_table_db2"""
}
