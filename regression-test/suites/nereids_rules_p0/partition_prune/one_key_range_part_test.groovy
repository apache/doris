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

suite("one_key_range_part_test") {

    String dbName = context.config.getDbNameByFile(context.file)
    sql """set partition_pruning_expand_threshold=1000;"""
    sql """set enable_fold_constant_by_be=false;"""

    sql """drop table if exists key_1_fixed_range_date_part;"""
    sql """create table key_1_fixed_range_date_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(dt) (
        PARTITION p_min VALUES LESS THAN ("2023-01-01 00:00:00"),
        PARTITION p_202301 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')),
        PARTITION p_202302 VALUES [('2023-02-01 00:00:00'), ('2023-03-01 00:00:00')),
        PARTITION p_202303 VALUES [('2023-03-01 00:00:00'), ('2023-04-01 00:00:00')),
        PARTITION p_202304 VALUES [('2023-04-01 00:00:00'), ('2023-05-01 00:00:00')),
        PARTITION p_202305 VALUES [('2023-05-01 00:00:00'), ('2023-06-01 00:00:00')),
        PARTITION p_202306 VALUES [('2023-06-01 00:00:00'), ('2023-07-01 00:00:00')),
        PARTITION p_202307 VALUES [('2023-07-01 00:00:00'), ('2023-08-01 00:00:00')),
        PARTITION p_202308 VALUES [('2023-08-01 00:00:00'), ('2023-09-01 00:00:00')),
        PARTITION p_202309 VALUES [('2023-09-01 00:00:00'), ('2023-10-01 00:00:00')),
        PARTITION p_202310 VALUES [('2023-10-01 00:00:00'), ('2023-11-01 00:00:00')),
        PARTITION p_202311 VALUES [('2023-11-01 00:00:00'), ('2023-12-01 00:00:00')),
        PARTITION p_202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00')),
        PARTITION p_max VALUES [('2024-01-01 00:00:00'), ('9999-12-31 23:59:59'))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_range_date_part values 
            (0, "2021-01-01 00:00:00", "000"),
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
            (13, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_fixed_range_date_part with sync;"""

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt = '2023-06-15 10:00:00';")
        contains "1/14 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt >= '2023-07-01 00:00:00' AND dt < '2023-08-01 00:00:00';")
        contains "1/14 (p_202307)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt BETWEEN '2023-09-01 00:00:00' AND '2023-10-10 00:00:00';")
        contains "2/14 (p_202309,p_202310)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt = '2023-08-01 00:00:00';")
        contains "1/14 (p_202308)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt < '2023-09-01 00:00:00' AND dt >= '2023-08-31 23:59:59';")
        contains "1/14 (p_202308)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-03-01 00:00:00';")
        contains "2/14 (p_202302,p_202303)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_trunc('month', dt) BETWEEN '2023-04-01 00:00:00' AND '2023-05-01 00:00:00';")
        contains "3/14 (p_202303,p_202304,p_202305)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) = '2023-11-15 10:00:00';")
        contains "2/14 (p_202310,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_sub(dt, INTERVAL 2 MONTH) > '2023-03-10 00:00:00';")
        contains "10/14 (p_min,p_202305,p_202306,p_202307,p_202308,p_202309,p_202310,p_202311,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE datediff(dt, '2023-04-01 00:00:00') = 10;")
        contains "14/14 (p_min,p_202301,p_202302,p_202303,p_202304,p_202305,p_202306,p_202307,p_202308,p_202309,p_202310,p_202311,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt = from_unixtime(unix_timestamp('2023-05-20 12:00:00'));")
        contains "14/14 (p_min,p_202301,p_202302,p_202303,p_202304,p_202305,p_202306,p_202307,p_202308,p_202309,p_202310,p_202311,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date(dt) = '2023-09-05';")
        contains "1/14 (p_202309)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE to_date(dt) = '2023-01-20';")
        contains "1/14 (p_202301)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE last_day(dt) = '2023-03-31 00:00:00';")
        contains "2/14 (p_202302,p_202303)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-08-01' OR a > 100;")
        contains "14/14 (p_min,p_202301,p_202302,p_202303,p_202304,p_202305,p_202306,p_202307,p_202308,p_202309,p_202310,p_202311,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) BETWEEN '2023-08-01' AND '2023-09-01';")
        contains "4/14 (p_202306,p_202307,p_202308,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE dt != '2023-07-05 12:00:00' AND dt > '2023-07-01' AND dt < '2023-08-01';")
        contains "1/14 (p_202307)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE (dt IS NULL OR dt < '2023-01-01') AND NOT (dt IS NULL);")
        contains "1/14 (p_min)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE CAST(dt AS DATE) = '2023-08-15' AND unix_timestamp(dt) > 1690848000;")
        contains "1/14 (p_202308)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_date_part WHERE (dt BETWEEN '2023-05-01' AND '2023-05-31' OR a = 1) AND (dt > '2023-06-15' OR c LIKE 'pattern');")
        contains "14/14 (p_min,p_202301,p_202302,p_202303,p_202304,p_202305,p_202306,p_202307,p_202308,p_202309,p_202310,p_202311,p_202312,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_date_part WHERE IF(DATE(dt) = DATE_SUB('2023-05-15 00:00:00', INTERVAL 1 MONTH), TRUE, FALSE);""")
        contains "1/14 (p_202304)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_date_part WHERE dt < '2023-04-01 00:00:00' AND IF(dt IS NOT NULL AND c = 'abc', TRUE, FALSE);""")
        contains "4/14 (p_min,p_202301,p_202302,p_202303)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_date_part WHERE (CASE WHEN dt < '2023-04-01 00:00:00' THEN dt ELSE dt - INTERVAL 1 MONTH END) > '2023-08-10 00:00:00';""")
        contains "14/14 (p_min,p_202301,p_202302,p_202303,p_202304,p_202305,p_202306,p_202307,p_202308,p_202309,p_202310,p_202311,p_202312,p_max)"
    }

    sql """drop table if exists key_1_special_fixed_range_date_part;"""
    sql """create table key_1_special_fixed_range_date_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(dt) (
        PARTITION p_min VALUES LESS THAN ("2023-01-01 00:00:00"),
        PARTITION p_202301 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')),
        PARTITION p_202302 VALUES [('2023-02-01 00:00:00'), ('2023-03-01 00:00:00')),
        PARTITION p_202304 VALUES [('2023-04-01 00:00:00'), ('2023-05-01 00:00:00')),
        PARTITION p_202305 VALUES [('2023-05-01 00:00:00'), ('2023-06-01 00:00:00')),
        PARTITION p_202306 VALUES [('2023-06-01 00:00:00'), ('2023-08-01 00:00:00')),
        PARTITION p_202309 VALUES [('2023-09-01 00:00:00'), ('2023-10-01 00:00:00')),
        PARTITION p_202310 VALUES [('2023-10-01 00:00:00'), ('2023-12-01 00:00:00')),
        PARTITION p_202312 VALUES [('2023-12-01 00:00:00'), ('2024-01-01 00:00:00')),
        PARTITION p_max VALUES [('2024-01-01 00:00:00'), ('9999-12-31 23:59:59'))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_special_fixed_range_date_part values 
            (0, "2021-01-01 00:00:00", "000"),
            (1, "2023-01-01 00:00:00", "111"),
            (2, "2023-02-01 00:00:00", "222"),
            (4, "2023-04-01 00:00:00", "444"),
            (5, "2023-05-01 00:00:00", "555"),
            (6, "2023-06-01 00:00:00", "666"),
            (9, "2023-09-01 00:00:00", "999"),
            (10, "2023-10-01 00:00:00", "jjj"),
            (12, "2023-12-01 00:00:00", "kkk"),
            (13, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_special_fixed_range_date_part with sync;"""

    sql """ALTER TABLE key_1_special_fixed_range_date_part ADD TEMPORARY PARTITION tp1 VALUES [("2023-03-01 00:00:00"), ("2023-04-01 00:00:00"));"""
    sql """insert into key_1_special_fixed_range_date_part TEMPORARY PARTITION(tp1) values (3, "2023-03-01 00:00:00", "333");"""

    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part TEMPORARY PARTITION(tp1) WHERE dt = '2023-03-15 10:00:00';")
        contains "1/10 (tp1)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = '2023-07-15 10:00:00';")
        contains "1/10 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt >= '2023-06-01 00:00:00' AND dt < '2023-08-01 00:00:00';")
        contains "1/10 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt BETWEEN '2023-09-05 00:00:00' AND '2023-11-10 00:00:00';")
        contains "2/10 (p_202309,p_202310)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = '2023-06-01 00:00:00';")
        contains "1/10 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt < '2023-08-01 00:00:00' AND dt >= '2023-07-31 23:59:59';")
        contains "1/10 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-03-01 00:00:00';")
        contains "1/10 (p_202302)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) BETWEEN '2023-06-01 00:00:00' AND '2023-07-01 00:00:00';")
        contains "2/10 (p_202305,p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) = '2023-11-15 10:00:00';")
        contains "2/10 (p_202310,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_sub(dt, INTERVAL 2 MONTH) > '2023-03-10 00:00:00';")
        contains "7/10 (p_min,p_202305,p_202306,p_202309,p_202310,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE datediff(dt, '2023-07-01 00:00:00') = 10;")
        contains "10/10 (p_min,p_202301,p_202302,p_202304,p_202305,p_202306,p_202309,p_202310,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt = from_unixtime(unix_timestamp('2023-11-20 12:00:00'));")
        contains "10/10 (p_min,p_202301,p_202302,p_202304,p_202305,p_202306,p_202309,p_202310,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date(dt) = '2023-09-05';")
        contains "1/10 (p_202309)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE to_date(dt) = '2023-01-20';")
        contains "1/10 (p_202301)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE last_day(dt) = '2023-07-31 00:00:00';")
        contains "1/10 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_trunc('month', dt) = '2023-08-01' OR a > 100;")
        contains "10/10 (p_min,p_202301,p_202302,p_202304,p_202305,p_202306,p_202309,p_202310,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE date_add(dt, INTERVAL 1 MONTH) BETWEEN '2023-07-01' AND '2023-09-01';")
        contains "3/10 (p_202305,p_202306,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE dt != '2023-07-05 12:00:00' AND dt > '2023-07-01' AND dt < '2023-08-01';")
        contains "1/10 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt IS NULL OR dt < '2023-01-01') AND NOT (dt IS NULL);")
        contains "1/10 (p_min)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE CAST(dt AS DATE) = '2023-06-01' AND unix_timestamp(dt) >= 1685548800;")
        contains "1/10 (p_202306)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt BETWEEN '2023-05-01' AND '2023-05-31' OR a = 1) AND (dt > '2023-06-15' OR c LIKE 'pattern');")
        contains "10/10 (p_min,p_202301,p_202302,p_202304,p_202305,p_202306,p_202309,p_202310,p_202312,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_date_part WHERE (dt BETWEEN '2023-05-01' AND '2023-05-31') AND (dt > '2023-06-15' OR c LIKE 'pattern');")
        contains "1/10 (p_202305)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_date_part WHERE IF(dt BETWEEN '2023-04-01 00:00:00' AND '2023-05-01 00:00:00', TRUE, FALSE);""")
        contains "2/10 (p_202304,p_202305)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_date_part WHERE IF(DATE(dt) = DATE_SUB('2023-10-15 00:00:00', INTERVAL 1 MONTH), TRUE, FALSE);""")
        contains "1/10 (p_202309)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_date_part WHERE (CASE WHEN dt < '2023-07-01 00:00:00' THEN dt ELSE dt - INTERVAL 1 MONTH END) > '2023-05-01 00:00:00';""")
        contains "10/10 (p_min,p_202301,p_202302,p_202304,p_202305,p_202306,p_202309,p_202310,p_202312,p_max)"
    }

    sql """drop table if exists key_1_fixed_range_int_part;"""
    sql """create table key_1_fixed_range_int_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(a) (
        PARTITION p_min VALUES [(-2147483648), (0)),
        PARTITION p_0_10 VALUES [(0), (10)),
        PARTITION p_10_20 VALUES [(10), (20)),
        PARTITION p_20_30 VALUES [(20), (30)),
        PARTITION p_30_40 VALUES [(30), (40)),
        PARTITION p_40_50 VALUES [(40), (50)),
        PARTITION p_50_60 VALUES [(50), (60)),
        PARTITION p_60_70 VALUES [(60), (70)),
        PARTITION p_70_80 VALUES [(70), (80)),
        PARTITION p_80_90 VALUES [(80), (90)),
        PARTITION p_90_100 VALUES [(90), (100)),
        PARTITION p_100_110 VALUES [(100), (110)),
        PARTITION p_max VALUES [(130), (2147483647))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_fixed_range_int_part values 
            (-10000, "2021-01-01 00:00:00", "000"),
            (0, "2021-01-01 00:00:00", "000"),
            (10, "2023-01-01 00:00:00", "111"),
            (20, "2023-02-01 00:00:00", "222"),
            (30, "2023-03-01 00:00:00", "333"),
            (40, "2023-04-01 00:00:00", "444"),
            (50, "2023-05-01 00:00:00", "555"),
            (60, "2023-06-01 00:00:00", "666"),
            (70, "2023-07-01 00:00:00", "777"),
            (80, "2023-08-01 00:00:00", "888"),
            (90, "2023-09-01 00:00:00", "999"),
            (100, "2023-10-01 00:00:00", "jjj"),
            (500000, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_fixed_range_int_part with sync;"""

    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a = 25;")
        contains "1/13 (p_20_30)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a >= 50 AND a < 60;")
        contains "1/13 (p_50_60)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a BETWEEN 45 AND 55;")
        contains "2/13 (p_40_50,p_50_60)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a = 10;")
        contains "1/13 (p_10_20)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a < 20 AND a >= 19;")
        contains "1/13 (p_10_20)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a + 10 = 35;")
        contains "1/13 (p_20_30)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a - 5 > 80;")
        contains "4/13 (p_80_90,p_90_100,p_100_110,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE CASE WHEN c = 'test' THEN a > 90 ELSE a < 10 END;")
        contains "13/13 (p_min,p_0_10,p_10_20,p_20_30,p_30_40,p_40_50,p_50_60,p_60_70,p_70_80,p_80_90,p_90_100,p_100_110,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_int_part WHERE a > IF(c IS NULL, 50, 10) AND dt IS NOT NULL;""")
        contains "13/13 (p_min,p_0_10,p_10_20,p_20_30,p_30_40,p_40_50,p_50_60,p_60_70,p_70_80,p_80_90,p_90_100,p_100_110,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_int_part WHERE a < (CASE WHEN a > 50 THEN 100 ELSE 20 END) OR a <=> 125""")
        contains "9/13 (p_min,p_0_10,p_10_20,p_50_60,p_60_70,p_70_80,p_80_90,p_90_100,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_int_part WHERE a BETWEEN 15 AND 18 AND CASE WHEN dt > '2023-01-01 00:00:00' THEN TRUE ELSE FALSE END;""")
        contains "1/13 (p_10_20)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_int_part WHERE CASE WHEN a > 50 THEN 'large_range' ELSE 'small_range' END = 'large_range';""")
        contains "7/13 (p_50_60,p_60_70,p_70_80,p_80_90,p_90_100,p_100_110,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_fixed_range_int_part WHERE (CASE WHEN a < 25 THEN a * 2 ELSE a / 2 END) > 30;""")
        contains "9/13 (p_min,p_10_20,p_20_30,p_60_70,p_70_80,p_80_90,p_90_100,p_100_110,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE MOD(a, 10) = 5;")
        contains "13/13 (p_min,p_0_10,p_10_20,p_20_30,p_30_40,p_40_50,p_50_60,p_60_70,p_70_80,p_80_90,p_90_100,p_100_110,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE ABS(a) < 10;")
        contains "3/13 (p_min,p_0_10,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE (a > 55 OR c = 'something') AND a < 65;")
        contains "8/13 (p_min,p_0_10,p_10_20,p_20_30,p_30_40,p_40_50,p_50_60,p_60_70)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a > 90 OR c LIKE 'test%';")
        contains "13/13 (p_min,p_0_10,p_10_20,p_20_30,p_30_40,p_40_50,p_50_60,p_60_70,p_70_80,p_80_90,p_90_100,p_100_110,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a != 25 AND a >= 20 AND a < 30;")
        contains "1/13 (p_20_30)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE (a IS NULL OR a < 0) AND NOT (a IS NULL);")
        contains "1/13 (p_min)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a BETWEEN 110 AND 129;")
        contains "0:VEMPTYSET"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_fixed_range_int_part WHERE a BETWEEN 105 AND 135;")
        contains "2/13 (p_100_110,p_max)"
    }


    sql """drop table if exists key_1_special_fixed_range_int_part;"""
    sql """create table key_1_special_fixed_range_int_part (a int, dt datetime, c varchar(100)) duplicate key(a)
    partition by range(a) (
        PARTITION p_min VALUES [(-2147483648), (0)),
        PARTITION p_0_10 VALUES [(0), (10)),
        PARTITION p_10_20 VALUES [(10), (20)),
        PARTITION p_30_60 VALUES [(30), (60)),
        PARTITION p_70_80 VALUES [(70), (90)),
        PARTITION p_90_100 VALUES [(90), (100)),
        PARTITION p_100_110 VALUES [(100), (110)),
        PARTITION p_120_130 VALUES [(120), (130)),
        PARTITION p_max VALUES [(130), (2147483647))
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """insert into key_1_special_fixed_range_int_part values 
            (-10000, "2021-01-01 00:00:00", "000"),
            (0, "2021-01-01 00:00:00", "000"),
            (10, "2023-01-01 00:00:00", "111"),
            (30, "2023-02-01 00:00:00", "222"),
            (70, "2023-03-01 00:00:00", "333"),
            (90, "2023-04-01 00:00:00", "444"),
            (100, "2023-05-01 00:00:00", "555"),
            (120, "2023-06-01 00:00:00", "666"),
            (500000, "2024-12-01 00:00:00", "aaa"),
            (null, null, null),
            (1, null, null),
            (null, "2023-01-01 00:00:00", null),
            (null, null, "zzz");"""
    sql """analyze table key_1_special_fixed_range_int_part with sync;"""

    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 45;")
        contains "1/9 (p_30_60)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a >= 30 AND a < 60;")
        contains "1/9 (p_30_60)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 95 AND 105;")
        contains "2/9 (p_90_100,p_100_110)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 30;")
        contains "1/9 (p_30_60)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a < 90 AND a >= 89;")
        contains "1/9 (p_70_80)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a + 10 = 45;")
        contains "1/9 (p_30_60)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a - 5 > 80;")
        contains "5/9 (p_70_80,p_90_100,p_100_110,p_120_130,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE CASE WHEN c = 'test' THEN a > 90 ELSE a < 10 END;")
        contains "9/9 (p_min,p_0_10,p_10_20,p_30_60,p_70_80,p_90_100,p_100_110,p_120_130,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_int_part WHERE a > IF(c IS NULL, 50, 10) AND dt IS NOT NULL;""")
        contains "9/9 (p_min,p_0_10,p_10_20,p_30_60,p_70_80,p_90_100,p_100_110,p_120_130,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_int_part WHERE a < (CASE WHEN a > 50 THEN 100 ELSE 20 END) OR a <=> 125""")
        contains "8/9 (p_min,p_0_10,p_10_20,p_30_60,p_70_80,p_90_100,p_120_130,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE MOD(a, 10) = 5;")
        contains "9/9 (p_min,p_0_10,p_10_20,p_30_60,p_70_80,p_90_100,p_100_110,p_120_130,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE ABS(a) < 10;")
        contains "3/9 (p_min,p_0_10,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE (a > 75 OR c = 'something') AND a < 85;")
        contains "5/9 (p_min,p_0_10,p_10_20,p_30_60,p_70_80)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a > 90 OR c LIKE 'test%';")
        contains "9/9 (p_min,p_0_10,p_10_20,p_30_60,p_70_80,p_90_100,p_100_110,p_120_130,p_max)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a != 45 AND a >= 30 AND a < 60;")
        contains "1/9 (p_30_60)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE (a IS NULL OR a < 0) AND NOT (a IS NULL);")
        contains "1/9 (p_min)"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 60 AND 69;")
        contains "0:VEMPTYSET"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a = 65;")
        contains "0:VEMPTYSET"
    }
    explain {
        sql("SELECT a, dt, c FROM key_1_special_fixed_range_int_part WHERE a BETWEEN 105 AND 125;")
        contains "2/9 (p_100_110,p_120_130)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_int_part WHERE CASE WHEN a >= 45 THEN TRUE ELSE FALSE END;""")
        contains "6/9 (p_30_60,p_70_80,p_90_100,p_100_110,p_120_130,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_int_part WHERE (CASE WHEN a < 50 THEN a + 10 ELSE a / 2 END) > 40;""")
        contains "7/9 (p_min,p_30_60,p_70_80,p_90_100,p_100_110,p_120_130,p_max)"
    }
    explain {
        sql("""SELECT * FROM key_1_special_fixed_range_int_part WHERE IF(a < 20 OR a > 90, TRUE, FALSE);""")
        contains "7/9 (p_min,p_0_10,p_10_20,p_90_100,p_100_110,p_120_130,p_max)"
    }


}
