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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_index_change_7") {
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }
    
    def tableName = "test_index_change_7"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
	    CREATE TABLE IF NOT EXISTS ${tableName}(
	    	`id`int(11)NULL,
	    	`int_array` array<int(20)> NULL,
	    	`c_array` array<varchar(20)> NULL,
	    	INDEX c_array_idx(`c_array`) USING INVERTED PROPERTIES("parser"="english") COMMENT 'c_array index',
	    	INDEX int_array_idx(`int_array`) USING INVERTED COMMENT 'int_array index'
	    ) ENGINE=OLAP
	    DUPLICATE KEY(`id`)
	    COMMENT 'OLAP'
	    DISTRIBUTED BY HASH(`id`) BUCKETS 1
	    PROPERTIES(
 	    	"replication_allocation" = "tag.location.default: 1"
	    );
        """

    sql "INSERT INTO ${tableName} VALUES (1, [10,20,30], ['i','love','china']), (2, [20,30,40], ['i','love','north korea']), (3, [30,40,50], NULL);"
    sql "INSERT INTO ${tableName} VALUES (4, [40,50,60], NULL);"
    
    qt_select1 """ SELECT * FROM ${tableName} t ORDER BY id; """
    qt_select2 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'china' ORDER BY id; """
    qt_select3 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'love' ORDER BY id; """
    qt_select4 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'north' ORDER BY id; """
    qt_select5 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'korea' ORDER BY id; """
    qt_select6 """ SELECT * FROM ${tableName} t WHERE int_array element_ge 40 ORDER BY id; """
    qt_select7 """ SELECT * FROM ${tableName} t WHERE int_array element_le 40 ORDER BY id; """
    qt_select8 """ SELECT * FROM ${tableName} t WHERE int_array element_gt 40 ORDER BY id; """
    qt_select9 """ SELECT * FROM ${tableName} t WHERE int_array element_lt 40 ORDER BY id; """
    qt_select10 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 10 ORDER BY id; """
    qt_select11 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 20 ORDER BY id; """
    qt_select12 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 30 ORDER BY id; """
    qt_select13 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 40 ORDER BY id; """
    qt_select14 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 50 ORDER BY id; """
    qt_select15 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 60 ORDER BY id; """

    // drop inverted index c_array_idx, int_array_idx
    sql """ DROP INDEX c_array_idx ON ${tableName} """
    sql """ DROP INDEX int_array_idx ON ${tableName} """

    // create inverted index
    sql """ CREATE INDEX c_array_idx ON ${tableName}(`c_array`) USING INVERTED PROPERTIES("parser"="english") """
    sql """ CREATE INDEX int_array_idx ON ${tableName}(`int_array`) USING INVERTED """

    // build inverted index
    sql """ BUILD INDEX c_array_idx ON ${tableName} """
    sql """ BUILD INDEX int_array_idx ON ${tableName} """
    wait_for_build_index_on_partition_finish(tableName, timeout)

    def show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 2)
    assertEquals(show_result[0][2], "c_array_idx")
    assertEquals(show_result[1][2], "int_array_idx")

    qt_select16 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'china' ORDER BY id; """
    qt_select17 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'love' ORDER BY id; """
    qt_select18 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'north' ORDER BY id; """
    qt_select19 """ SELECT * FROM ${tableName} t WHERE c_array MATCH 'korea' ORDER BY id; """

    try {
        qt_select20 """ SELECT * FROM ${tableName} t WHERE int_array element_ge 40 ORDER BY id; """
        qt_select21 """ SELECT * FROM ${tableName} t WHERE int_array element_le 40 ORDER BY id; """
        qt_select22 """ SELECT * FROM ${tableName} t WHERE int_array element_gt 40 ORDER BY id; """
        qt_select23 """ SELECT * FROM ${tableName} t WHERE int_array element_lt 40 ORDER BY id; """
        qt_select24 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 10 ORDER BY id; """
        qt_select25 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 20 ORDER BY id; """
        qt_select26 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 30 ORDER BY id; """
        qt_select27 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 40 ORDER BY id; """
        qt_select28 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 50 ORDER BY id; """
        qt_select29 """ SELECT * FROM ${tableName} t WHERE int_array element_eq 60 ORDER BY id; """
    } catch(Exception ex) {
        logger.info("execute array element query failed when build index not finished, result: " + ex)
    }
}
