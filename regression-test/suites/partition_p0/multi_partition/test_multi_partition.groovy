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

suite("test_multi_partition") {
    // todo: test multi partitions : create table partition ...
    sql "drop table if exists multi_par"
    sql """
        CREATE TABLE IF NOT EXISTS multi_par ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL,
            k13 datetimev2 NOT NULL,
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY RANGE(k10) ( 
            FROM  ("2022-12-01") TO ("2022-12-31") INTERVAL 1 DAY
            ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    List<List<Object>> result1  = sql "show tables like 'multi_par'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    List<List<Object>> result2  = sql "show partitions from multi_par"
    logger.info("${result2}")
    assertEquals(result2.size(), 30)
    sql "drop table multi_par"


    sql "drop table if exists multi_par1"
    sql """
        CREATE TABLE IF NOT EXISTS multi_par1 ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL,
            k13 datetimev2 NOT NULL,
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY RANGE(k10) ( 
            FROM ("2000-11-14") TO ("2021-11-14") INTERVAL 1 YEAR,
            FROM ("2021-11-14") TO ("2022-11-14") INTERVAL 1 MONTH,
            FROM ("2022-11-14") TO ("2023-01-03") INTERVAL 1 WEEK,
            FROM ("2023-01-03") TO ("2023-01-14") INTERVAL 1 DAY,
            PARTITION p_20230114 VALUES [('2023-01-14'), ('2023-01-15'))
            ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    result1  = sql "show tables like 'multi_par1'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    result2  = sql "show partitions from multi_par1"
    logger.info("${result2}")
    assertEquals(result2.size(), 55)
    sql "drop table multi_par1"


    sql "drop table if exists multi_par2"
    sql """
        CREATE TABLE IF NOT EXISTS multi_par2 ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL,
            k13 datetimev2 NOT NULL,
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY RANGE(k11) ( 
            FROM ("2022-12-01 02") TO ("2022-12-02 02") INTERVAL 1 HOUR,
            FROM ("2022-12-02 02:00:00") TO ("2022-12-03 02:00:00") INTERVAL 1 HOUR           
            ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    result1  = sql "show tables like 'multi_par2'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    result2  = sql "show partitions from multi_par2"
    logger.info("${result2}")
    assertEquals(result2.size(), 48)
    sql "drop table multi_par2"


    sql "drop table if exists multi_par3"
    sql """
        CREATE TABLE IF NOT EXISTS multi_par3 ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL,
            k13 datetimev2 NOT NULL,
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY RANGE(k11) ( 
            FROM ("2022-12-01 02") TO ("2022-12-02 02") INTERVAL 1 HOUR,
            FROM ("2022-12-02 02:00:00") TO ("2022-12-03 02:00:00") INTERVAL 1 HOUR           
            ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    result1  = sql "show tables like 'multi_par3'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    result2  = sql "show partitions from multi_par3"
    logger.info("${result2}")
    assertEquals(result2.size(), 48)
    sql "drop table multi_par3"


    sql "drop table if exists multi_par4"
    sql """
        CREATE TABLE IF NOT EXISTS multi_par4 ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL,
            k13 datetimev2 NOT NULL,
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY RANGE(k12) ( 
            FROM  ("2022-12-01") TO ("2022-12-31") INTERVAL 1 DAY
            ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    result1  = sql "show tables like 'multi_par4'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    result2  = sql "show partitions from multi_par4"
    logger.info("${result2}")
    assertEquals(result2.size(), 30)
    sql "drop table multi_par4"

    sql "drop table if exists multi_par5"
    sql """
        CREATE TABLE IF NOT EXISTS multi_par5 ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL,
            k13 datetimev2 NOT NULL,
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY RANGE(k13) ( 
            FROM ("2022-12-01 02") TO ("2022-12-02 02") INTERVAL 1 HOUR,
            FROM ("2022-12-02 02:00:00") TO ("2022-12-03 02:00:00") INTERVAL 1 HOUR           
            ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    result1  = sql "show tables like 'multi_par5'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    result2  = sql "show partitions from multi_par5"
    logger.info("${result2}")
    assertEquals(result2.size(), 48)
    sql "drop table multi_par5"
}
