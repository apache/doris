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

suite("one_key_list_part_update_test") {

    String dbName = context.config.getDbNameByFile(context.file)
    sql """set partition_pruning_expand_threshold=1000;"""
    sql """SET enable_insert_strict = false;"""
    sql """set enable_fold_constant_by_be=false;"""

    sql """drop table if exists key_1_fixed_list_int_part_update"""
    sql """create table key_1_fixed_list_int_part_update (a int, dt datetime, c varchar(100)) duplicate key(a)
        PARTITION BY LIST(a)
        (
            PARTITION `p_NULL` VALUES IN ((NULL)),
            PARTITION `p_1` VALUES IN ("1"),
            PARTITION `p_2` VALUES IN ("2"),
            PARTITION `p_3` VALUES IN ("3"),
            PARTITION `p_4` VALUES IN ("4"),
            PARTITION `p_5` VALUES IN ("5"),
            PARTITION `p_6` VALUES IN ("6"),
            PARTITION `p_7` VALUES IN ("7"),
            PARTITION `p_8` VALUES IN ("8"),
            PARTITION `p_9` VALUES IN ("9"),
            PARTITION `p_10` VALUES IN ("10"),
            PARTITION `p_11` VALUES IN ("11"),
            PARTITION `p_12` VALUES IN ("12"),
            PARTITION `p_13` VALUES IN ("13")
        ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_list_int_part_update values 
            (1, "2023-01-01 00:00:00", "111"),
            (2, "2023-02-01 00:00:00", "222"),
            (3, "2023-03-01 00:00:00", "333"),
            (4, "2023-04-01 00:00:00", "444"),
            (5, "2023-05-01 00:00:00", "555"),
            (6, "2023-06-01 00:00:00", "666"),
            (6, "2023-06-15 10:00:00", "666"),
            (7, "2023-07-01 00:00:00", "777"),
            (8, "2023-08-01 00:00:00", "888"),
            (9, "2023-09-01 00:00:00", "999"),
            (10, "2023-10-01 00:00:00", "jjj"),
            (11, "2023-11-01 00:00:00", "qqq"),
            (12, "2023-12-01 00:00:00", "kkk"),
            (13, "2023-12-02 00:00:00", "zzz"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            ("null", null, "zzz");"""
    sql """analyze table key_1_fixed_list_int_part_update with sync;"""

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a = 13;")
        contains "1/14 (p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a IN (1, 3, 8, 13);")
        contains "4/14 (p_1,p_3,p_8,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a IN (2, 12, 13);")
        contains "3/14 (p_2,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a IS NULL;")
        contains "1/14 (p_NULL)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a + 5 = 18;")
        contains "1/14 (p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a = CASE WHEN c = 'test' THEN 10 ELSE 5 END;")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE MOD(a, 3) = 1;")
        contains "13/14 (p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a IN (1, 2, 3, 13) AND c = 'test_value';")
        contains "4/14 (p_1,p_2,p_3,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a = 13 OR c LIKE 'pattern%';")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a NOT IN (5);")
        contains "12/14 (p_1,p_2,p_3,p_4,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a != 13;")
        contains "12/14 (p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a IN (1, 2, 3, 4, 5, 13) AND (a + 5 > 6) AND c IS NOT NULL;")
        contains "5/14 (p_2,p_3,p_4,p_5,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE (a IN (1, 5, 9, 13) OR c = 'unknown') AND dt IS NOT NULL;")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE a IN (CASE WHEN a > 5 THEN 7 ELSE 3 END,CASE WHEN c = 'special' THEN 11 ELSE 1 END);")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_int_part_update WHERE (a IN (1, 5, 9, 13)) AND (MOD(a, 2) = 1 OR a > 10);")
        contains "4/14 (p_1,p_5,p_9,p_13)"
    }
    explain {
        sql("SELECT * FROM key_1_fixed_list_int_part_update WHERE CASE WHEN a > 9 THEN 1 ELSE 0 END = 1;")
        contains "4/14 (p_10,p_11,p_12,p_13)"
    }

    explain {
        sql("SELECT * FROM key_1_fixed_list_int_part_update WHERE IF(a IN (1, 2) OR a > 10, TRUE, FALSE);")
        contains "5/14 (p_1,p_2,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT * FROM key_1_fixed_list_int_part_update WHERE (CASE WHEN a > 5 THEN a ELSE a * 2 END) % 4 = 0;")
        contains "13/14 (p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }

    sql """drop table if exists key_1_fixed_list_date_part_update"""
    sql """create table key_1_fixed_list_date_part_update (a int, dt datetime, c varchar(100)) duplicate key(a)
        PARTITION BY LIST(dt)
        (
            PARTITION `p_NULL` VALUES IN ((NULL)),
            PARTITION `p_1` VALUES IN ("2023-01-01 00:00:00"),
            PARTITION `p_2` VALUES IN ("2023-02-01 00:00:00"),
            PARTITION `p_3` VALUES IN ("2023-03-01 00:00:00"),
            PARTITION `p_4` VALUES IN ("2023-04-01 00:00:00"),
            PARTITION `p_5` VALUES IN ("2023-05-01 00:00:00"),
            PARTITION `p_6` VALUES IN ("2023-06-01 00:00:00"),
            PARTITION `p_7` VALUES IN ("2023-07-01 00:00:00"),
            PARTITION `p_8` VALUES IN ("2023-08-01 00:00:00"),
            PARTITION `p_9` VALUES IN ("2023-09-01 00:00:00"),
            PARTITION `p_10` VALUES IN ("2023-10-01 00:00:00"),
            PARTITION `p_11` VALUES IN ("2023-11-01 00:00:00"),
            PARTITION `p_12` VALUES IN ("2023-12-01 00:00:00"),
            PARTITION `p_13` VALUES IN ("2023-12-02 00:00:00")
        ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_list_date_part_update values 
            (1, "2023-01-01 00:00:00", "111"),
            (2, "2023-02-01 00:00:00", "222"),
            (3, "2023-03-01 00:00:00", "333"),
            (4, "2023-04-01 00:00:00", "444"),
            (5, "2023-05-01 00:00:00", "555"),
            (6, "2023-06-01 00:00:00", "666"),
            (7, "2023-07-01 00:00:00", "777"),
            (8, "2023-08-01 00:00:00", "888"),
            (9, "2023-09-01 00:00:00", "999"),
            (10, "2023-10-01 00:00:00", "jjj"),
            (11, "2023-11-01 00:00:00", "qqq"),
            (12, "2023-12-01 00:00:00", "kkk"),
            (13, "2023-12-02 00:00:00", "zzz"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, "null", "zzz");"""
    sql """analyze table key_1_fixed_list_date_part_update with sync;"""

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt = '2023-12-02 00:00:00';")
        contains "1/14 (p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt IN ('2023-01-01 00:00:00', '2023-03-01 00:00:00', '2023-12-02 00:00:00');")
        contains "3/14 (p_1,p_3,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt IN ('2023-02-01 00:00:00', '2023-12-02 00:00:00');")
        contains "2/14 (p_2,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt IS NULL;")
        contains "1/14 (p_NULL)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE date_add(dt, INTERVAL 1 MONTH) = '2024-01-02 00:00:00';")
        contains ""
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt = CASE WHEN a > 5 THEN '2023-10-01 00:00:00' ELSE '2023-03-01 00:00:00' END;")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE date(dt) = '2023-12-02';")
        contains "1/14 (p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE to_date(dt) = '2023-12-02';")
        contains "1/14 (p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt IN ('2023-01-01 00:00:00', '2023-02-01 00:00:00', '2023-12-02 00:00:00') AND c = 'test_value';")
        contains "3/14 (p_1,p_2,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt = '2023-12-02 00:00:00' OR c LIKE 'pattern%';")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt NOT IN ('2023-05-01 00:00:00');")
        contains "12/14 (p_1,p_2,p_3,p_4,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt != '2023-06-01 00:00:00';")
        contains "12/14 (p_1,p_2,p_3,p_4,p_5,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt IN ('2023-01-01 00:00:00', '2023-02-01 00:00:00', '2023-03-01 00:00:00', '2023-04-01 00:00:00', '2023-12-02 00:00:00') AND (a + 5 > 6) AND c IS NOT NULL;")
        contains "5/14 (p_1,p_2,p_3,p_4,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE (dt IN ('2023-01-01 00:00:00', '2023-05-01 00:00:00', '2023-13-02 00:00:00') OR c = 'unknown') AND a IS NOT NULL;")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE dt IN ( CASE WHEN a > 5 THEN '2023-07-01 00:00:00' ELSE '2023-03-01 00:00:00' END, CASE WHEN c = 'special' THEN '2023-11-01 00:00:00' ELSE '2023-01-01 00:00:00' END);")
        contains "14/14 (p_NULL,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_date_part_update WHERE (dt IN ('2023-01-01 00:00:00', '2023-05-01 00:00:00', '2023-09-01 00:00:00', '2023-12-02 00:00:00')) AND (a = 1 OR a > 10);")
        contains "4/14 (p_1,p_5,p_9,p_13)"
    }

    explain {
        sql("SELECT * FROM key_1_fixed_list_date_part_update WHERE IF(dt IN ('2023-11-01 00:00:00', '2024-01-01 00:00:00'), TRUE, FALSE);")
        contains "1/14 (p_11)"
    }
    explain {
        sql("SELECT * FROM key_1_fixed_list_date_part_update WHERE IF(dt IN ('2023-01-01 00:00:00', '2023-02-01 00:00:00') OR dt > '2023-10-01 00:00:00', TRUE, FALSE);")
        contains "5/14 (p_1,p_2,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT * FROM key_1_fixed_list_date_part_update WHERE IF(dt IS NULL OR dt = '2023-12-02 00:00:00', TRUE, FALSE);")
        contains "2/14 (p_NULL,p_13)"
    }

    sql """drop table if exists key_1_fixed_list_varchar_part_update"""
    sql """create table key_1_fixed_list_varchar_part_update (a int, dt datetime, c varchar(100)) duplicate key(a)
        PARTITION BY LIST(c)
        (
            PARTITION `p_NULL` VALUES IN ((NULL)), 
            PARTITION `p_NULL_2` VALUES IN ("NULL"),
            PARTITION `p_1` VALUES IN ("111"),
            PARTITION `p_2` VALUES IN ("222"),
            PARTITION `p_3` VALUES IN ("333"),
            PARTITION `p_4` VALUES IN ("4444"),
            PARTITION `p_5` VALUES IN ("555"),
            PARTITION `p_6` VALUES IN ("666"),
            PARTITION `p_7` VALUES IN ("7777"),
            PARTITION `p_8` VALUES IN ("888"),
            PARTITION `p_9` VALUES IN ("999"),
            PARTITION `p_10` VALUES IN ("jjj"),
            PARTITION `p_11` VALUES IN ("qqq"),
            PARTITION `p_12` VALUES IN ("kkk"),
            PARTITION `p_13` VALUES IN ("aaa")
        ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_list_varchar_part_update values 
            (100, "2023-01-01 00:00:00", "111"),
            (200, "2023-02-01 00:00:00", "222"),
            (300, "2023-03-01 00:00:00", "333"),
            (400, "2023-04-01 00:00:00", "4444"),
            (500, "2023-05-01 00:00:00", "555"),
            (600, "2023-06-01 00:00:00", "666"),
            (700, "2023-07-01 00:00:00", "7777"),
            (800, "2023-08-01 00:00:00", "888"),
            (900, "2023-09-01 00:00:00", "999"),
            (1000, "2023-10-01 00:00:00", "jjj"),
            (500000, "2024-12-01 00:00:00", "qqq"),
            (1000000, "2024-12-01 00:00:00", "kkk"),
            (200000, "2024-12-02 00:00:00", "aaa"),
            (null, null, "NULL"),
            (1, null, "NULL"),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "kkk");"""
    sql """analyze table key_1_fixed_list_varchar_part_update with sync;"""

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c = 'aaa';")
        contains "1/15 (p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c IN ('111', '333', 'aaa');")
        contains "3/15 (p_1,p_3,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c IN ('222', 'kkk', 'aaa');")
        contains "3/15 (p_2,p_12,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c IS NULL;")
        contains "1/15 (p_NULL)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE LOWER(c) = 'aaa';")
        contains "1/15 (p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE LENGTH(c) = 3;")
        contains "11/15 (p_1,p_2,p_3,p_5,p_6,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE CONCAT(c, 'suffix') = 'aaasuffix';")
        contains "1/15 (p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c IN ('111', '222', 'aaa') AND a = 10;")
        contains "3/15 (p_1,p_2,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c = 'aaa' OR dt IS NULL;")
        contains "15/15 (p_NULL,p_NULL_2,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c NOT IN ('555');")
        contains "13/15 (p_NULL_2,p_1,p_2,p_3,p_4,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c != '666';")
        contains "13/15 (p_NULL_2,p_1,p_2,p_3,p_4,p_5,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c IN ('111', '222', '333', '444', 'aaa') AND a > 1 AND dt IS NOT NULL;")
        contains "4/15 (p_1,p_2,p_3,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE (c IN ('111', '555', 'aaa') OR dt IS NULL) AND a IS NOT NULL;")
        contains "15/15 (p_NULL,p_NULL_2,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE c IN (CASE WHEN a > 5 THEN '777' ELSE '333' END,CASE WHEN a = 10 THEN 'kkk' ELSE '111' END);")
        contains "15/15 (p_NULL,p_NULL_2,p_1,p_2,p_3,p_4,p_5,p_6,p_7,p_8,p_9,p_10,p_11,p_12,p_13)"
    }

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_list_varchar_part_update WHERE (c IN ('111', '555', 'aaa', 'kkk')) AND (a = 1 OR a > 10);")
        contains "4/15 (p_1,p_5,p_12,p_13)"
    }
    explain {
        sql("SELECT * FROM key_1_fixed_list_varchar_part_update WHERE CASE WHEN c > 'a' THEN 1 ELSE 0 END = 1;")
        contains "4/15 (p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT * FROM key_1_fixed_list_varchar_part_update WHERE IF(c IN ('111', '222') OR c > 'a', TRUE, FALSE);")
        contains "6/15 (p_1,p_2,p_10,p_11,p_12,p_13)"
    }
    explain {
        sql("SELECT * FROM key_1_fixed_list_varchar_part_update WHERE CASE WHEN LENGTH(c) = 4 THEN TRUE ELSE FALSE END;")
        contains "3/15 (p_NULL_2,p_4,p_7)"
    }

}
