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

suite("test_unique_table_auto_inc_partial_update_correct_stream_load") {

    def backends = sql_return_maparray('show backends')
    def replicaNum = 0
    def targetBackend = null
    for (def be : backends) {
        def alive = be.Alive.toBoolean()
        def decommissioned = be.SystemDecommissioned.toBoolean()
        if (alive && !decommissioned) {
            replicaNum++
            targetBackend = be
        }
    }
    assertTrue(replicaNum > 0)
    
    def check_data_correct = { def tableName ->
        def old_result = sql "select id from ${tableName} order by id;"
        logger.info("first result: " + old_result)
        for (int i = 1; i<30; ++i){
            def new_result = sql "select id from ${tableName} order by id;"
            logger.info("new result: " + new_result)
            for (int j = 0; j<old_result.size();++j){
                if (old_result[j][0]!=new_result[j][0]){
                    logger.info("table name: " + tableName)
                    logger.info("old result: " + old_result)
                    logger.info("new result: " + new_result)
                    assertTrue(false)
                }
            }
            old_result = new_result
        }
    }
    // test for partial update, auto inc col is key
    def table1 = "unique_auto_inc_col_key_partial_update_stream_load"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
        "replication_num" = "${replicaNum}",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    // Bob, 100
    // Alice, 200
    // Tom, 300
    // Test, 400
    // Carter, 500
    // Smith, 600
    // Beata, 700
    // Doris, 800
    // Nereids, 900
    streamLoad {
        table "${table1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_select1_1 "select * from ${table1} order by id;"

    // 1, 123
    // 3, 323
    // 5, 523
    // 7, 723
    // 9, 923
    streamLoad {
        table "${table1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update1.csv'
        time 10000
    }
    sql "sync"
    qt_select1_1 "select name, value from ${table1} order by name, value;"
    qt_select1_2 "select id, count(*) from ${table1} group by id having count(*) > 1;"
    check_data_correct(table1)
    sql "drop table if exists ${table1};"

    // test for partial update, auto inc col is value, update auto inc col
    def table2 = "unique_auto_inc_col_value_partial_update_stream_load"
    sql "drop table if exists ${table2}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table2}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`) BUCKETS 3
        PROPERTIES (
        "replication_num" = "${replicaNum}",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """

    // Bob, 100
    // Alice, 200
    // Tom, 300
    // Test, 400
    // Carter, 500
    // Smith, 600
    // Beata, 700
    // Doris, 800
    // Nereids, 900
    streamLoad {
        table "${table2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_select2_1 "select name, value from ${table2} order by name, value;"
    qt_select2_2 "select id, count(*) from ${table2} group by id having count(*) > 1;"
    check_data_correct(table2)

    // Bob, 9990
    // Tom, 9992
    // Carter, 9994
    // Beata, 9996
    // Nereids, 9998
    streamLoad {
        table "${table2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, id'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update2.csv'
        time 10000
    }
    sql "sync"
    qt_select2_3 "select name, value from ${table2} order by name, value;"
    qt_select2_4 "select id, count(*) from ${table2} group by id having count(*) > 1;"
    check_data_correct(table2)
    sql "drop table if exists ${table2};"

    // test for partial update, auto inc col is value, update other col
    def table3 = "unique_auto_inc_col_value_partial_update_stream_load"
    sql "drop table if exists ${table3}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table3}` (
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `value` int(11) NOT NULL COMMENT "用户得分",
          `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT "用户 ID"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`) BUCKETS 3
        PROPERTIES (
        "replication_num" = "${replicaNum}",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true"
        )
    """
    streamLoad {
        table "${table3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'

        file 'auto_inc_basic.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_select3_1 "select name, value from ${table3} order by name, value;"
    qt_select3_2 "select id, count(*) from ${table3} group by id having count(*) > 1;"
    check_data_correct(table3)

    // Bob, 9990
    // Tom, 9992
    // Carter, 9994
    // Beata, 9996
    // Nereids, 9998
    streamLoad {
        table "${table3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update2.csv'
        time 10000
    }
    sql "sync"
    qt_select3_3 "select name, value from ${table3} order by name, value;"
    qt_select3_4 "select id, count(*) from ${table3} group by id having count(*) > 1;"
    check_data_correct(table3)
    // BBob, 9990
    // TTom, 9992
    // CCarter, 9994
    // BBeata, 9996
    // NNereids, 9998
    streamLoad {
        table "${table3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'name, value'
        set 'partial_columns', 'true'

        file 'auto_inc_partial_update3.csv'
        time 10000
    }
    sql "sync"
    qt_select3_5 "select name, value from ${table3} order by name, value;"
    qt_select3_6 "select id, count(*) from ${table3} group by id having count(*) > 1;"
    check_data_correct(table3)
    sql "drop table if exists ${table3};"
}

