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


suite("test_count_on_index2", "p0") {
    // prepare test table

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName1 = "test_count_on_index2_1"
    def indexTblName2 = "test_count_on_index2_2"

    sql "DROP TABLE IF EXISTS ${indexTblName1}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName1}(
		`id` int(11) NULL,
		`a` string NULL,
        `b` string NULL,
        `c` int(11) NULL,
        `d` int(11) NULL,
        INDEX a_idx(`a`) USING INVERTED COMMENT '',
        INDEX b_idx(`b`) USING INVERTED COMMENT '',
        INDEX c_idx(`c`) USING INVERTED COMMENT '',
        INDEX d_idx(`d`) USING INVERTED COMMENT ''
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 		"replication_allocation" = "tag.location.default: 1"
	);
    """

    sql "DROP TABLE IF EXISTS ${indexTblName2}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName2}(
		`id` int(11) NULL,
		`a` string NULL,
        `b` string NULL,
        `c` int(11) NULL,
        `d` int(11) NULL
	) ENGINE=OLAP
	DUPLICATE KEY(`id`)
	COMMENT 'OLAP'
	DISTRIBUTED BY HASH(`id`) BUCKETS 1
	PROPERTIES(
 		"replication_allocation" = "tag.location.default: 1"
	);
    """
    
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    for(int i = 0; i <= 10; i++) {
        sql """INSERT INTO ${indexTblName1} VALUES
            (1, '1', '4', 7, 11),
            (2, '1', '4', 8, 11),
            (3, '1', '5', 8, 11),
            (4, '2', '5', 8, 12),
            (5, '2', '5', 9, 12),
            (6, '2', '6', 9, 12),
            (7, '3', '6', 9, 13),
            (8, '3', '6', 10, 13),
            (9, '3', '7', 10, 13),
            (10, '4', '7', 10, 14);
        """

        sql """INSERT INTO ${indexTblName2} VALUES
            (1, '1', '4', 7, 11),
            (2, '1', '4', 8, 11),
            (3, '1', '5', 8, 11),
            (4, '2', '5', 8, 12),
            (5, '2', '5', 9, 12),
            (6, '2', '6', 9, 12),
            (7, '3', '6', 9, 13),
            (8, '3', '6', 10, 13),
            (9, '3', '7', 10, 13),
            (10, '4', '7', 10, 14);
        """
    }

    qt_sql "select count() from ${indexTblName1} where (`id` >= 2 and `id` <= 8) and (`a` = '2' and `b` = '5' and c = 9 and d = 12);";
    qt_sql "select count() from ${indexTblName2} where (`id` >= 2 and `id` <= 8) and (`a` = '2' and `b` = '5' and c = 9 and d = 12);";

    qt_sql "select count() from ${indexTblName1} where (`id` >= 2 and `id` <= 8) and (`a` = '2' and `b` = '5' or c = 9 and d = 12);";
    qt_sql "select count() from ${indexTblName2} where (`id` >= 2 and `id` <= 8) and (`a` = '2' and `b` = '5' or c = 9 and d = 12);";

    qt_sql "select count() from ${indexTblName1} where (`id` >= 2 and `id` <= 8) and (`a` = '2' or `b` = '5' and c = 9 and d = 12);";
    qt_sql "select count() from ${indexTblName2} where (`id` >= 2 and `id` <= 8) and (`a` = '2' or `b` = '5' and c = 9 and d = 12);";

    qt_sql "select count() from ${indexTblName1} where (`id` >= 2 and `id` <= 8) and (`a` = '2' or `b` = '5' and c = 9 or d = 12);";
    qt_sql "select count() from ${indexTblName2} where (`id` >= 2 and `id` <= 8) and (`a` = '2' or `b` = '5' and c = 9 or d = 12);";

    qt_sql "select count() from ${indexTblName1} where (`id` >= 2 and `id` <= 8) and (`a` = '2' and `b` = '5' or c = 9 or d = 12);";
    qt_sql "select count() from ${indexTblName2} where (`id` >= 2 and `id` <= 8) and (`a` = '2' and `b` = '5' or c = 9 or d = 12);";

    qt_sql "select count() from ${indexTblName1} where (`id` >= 2 and `id` <= 8) and (`a` = '2' or `b` = '5' or c = 9 or d = 12);";
    qt_sql "select count() from ${indexTblName2} where (`id` >= 2 and `id` <= 8) and (`a` = '2' or `b` = '5' or c = 9 or d = 12);";

    qt_sql "select count() from ${indexTblName1} where `id` >= 2 and `id` <= 8 or (`a` = '2' or `b` = '5' or c = 9 or d = 12);";
    qt_sql "select count() from ${indexTblName2} where `id` >= 2 and `id` <= 8 or (`a` = '2' or `b` = '5' or c = 9 or d = 12);";

    qt_sql "select count() from ${indexTblName1} where `id` >= 2 or `id` <= 8 or (`a` = '2' or `b` = '5' or c = 9 or d = 12);";
    qt_sql "select count() from ${indexTblName2} where `id` >= 2 or `id` <= 8 or (`a` = '2' or `b` = '5' or c = 9 or d = 12);";

    qt_sql "select count() from ${indexTblName1} where `a` = '2' or `b` = '5' or c = 9 or d = 12;";
    qt_sql "select count() from ${indexTblName2} where `a` = '2' or `b` = '5' or c = 9 or d = 12;";
}
