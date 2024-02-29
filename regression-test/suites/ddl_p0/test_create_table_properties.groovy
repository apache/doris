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

// this suite is for creating table with timestamp datatype in defferent 
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("test_create_table_properties") {    
	try {
		sql """
        create table test_create_table_properties (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `start_time` DATETIME,
            `billing_cycle_id` INT
        )
        partition by range(`billing_cycle_id`, `start_time`)(
            PARTITION p202201_otr VALUES [("202201", '2000-01-01 00:00:00'), ("202201", '2022-01-01 00:00:00')),
            PARTITION error_partition VALUES [("999999", '1970-01-01 00:00:00'), ("999999", '1970-01-02 00:00:00'))
        )
        distributed by hash(`user_id`) buckets 1
        properties(
            "replication_num"="1",
            "abc"="false"
        );
			"""
        assertTrue(false, "should not be able to execute")
	}
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Unknown properties"))
	} finally {
        sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    }

    def cooldown_ttl = "10"
    def create_s3_resource = try_sql """
        CREATE RESOURCE IF NOT EXISTS "test_create_table_use_resource"
        PROPERTIES(
            "type"="s3",
            "AWS_REGION" = "bj",
            "AWS_ENDPOINT" = "bj.s3.comaaaa",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_BUCKET" = "test-bucket",
            "s3_validity_check" = "false"
        );
    """
    def create_succ_1 = try_sql """
        CREATE STORAGE POLICY IF NOT EXISTS test_create_table_use_policy
        PROPERTIES(
        "storage_resource" = "test_create_table_use_resource",
        "cooldown_ttl" = "$cooldown_ttl"
        );
    """

    def create_table_use_created_policy = try_sql """
	    create table if not exists test_create_table_properties (
			`user_id` LARGEINT NOT NULL COMMENT "用户id",
			`start_time` DATETIME,
			`billing_cycle_id` INT
		)
		partition by range(`billing_cycle_id`, `start_time`)(
			PARTITION p202201_otr VALUES [("202201", '2000-01-01 00:00:00'), ("202201", '2022-01-01 00:00:00')),
			PARTITION error_partition VALUES [("999999", '1970-01-01 00:00:00'), ("999999", '1970-01-02 00:00:00'))
		)
		distributed by hash(`user_id`) buckets 1
		properties(
			"replication_num"="1",
			"storage_policy" = "test_create_table_use_policy"
		);
    """

    sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    
	try {
        sql """
        create table test_create_table_properties (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
			`start_time` DATETIME,
			`billing_cycle_id` INT
		)
		partition by range(`billing_cycle_id`, `start_time`)(
			PARTITION p202201_otr VALUES [("202201", '2000-01-01 00:00:00'), ("202201", '2022-01-01 00:00:00')),
			PARTITION error_partition VALUES [("999999", '1970-01-01 00:00:00'), ("999999", '1970-01-02 00:00:00'))
		)
		distributed by hash(`user_id`) buckets 1
		properties(
			"replication_num"="1",
			"storage_policy" = "test_create_table_use_policy",
			"abc"="false"
		);
			"""
        assertTrue(false, "should not be able to execute")
	}
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Unknown properties"))
	} finally {
        sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    }

    try {
        sql """
        create table test_create_table_properties (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
			`start_time` DATETIME,
			`billing_cycle_id` INT
		)
		partition by range(`billing_cycle_id`, `start_time`)(
			PARTITION p202201_otr VALUES [("202201", '2000-01-01 00:00:00'), ("202201", '2022-01-01 00:00:00')),
			PARTITION error_partition VALUES [("999999", '1970-01-01 00:00:00'), ("999999", '1970-01-02 00:00:00'))
		)
		distributed by hash(`user_id`) buckets 1
		properties(
			"replication_num"="1",
			"storage_policy" = "test_create_table_use_policy",
            "dynamic_partition.start" = "-7"
		);
			"""
        assertTrue(false, "should not be able to execute")
	}
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Dynamic partition only support single-column range partition"))
	} finally {
        sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    }

    try {
        sql """
        create table test_create_table_properties (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
			`start_time` DATETIME,
			`billing_cycle_id` INT
		)
		partition by range(`billing_cycle_id`, `start_time`)(
			PARTITION p202201_otr VALUES [("202201", '2000-01-01 00:00:00'), ("202201", '2022-01-01 00:00:00')),
			PARTITION error_partition VALUES [("999999", '1970-01-01 00:00:00'), ("999999", '1970-01-02 00:00:00'))
		)
		distributed by hash(`user_id`) buckets 1
		properties(
			"replication_num"="1",
			"storage_policy" = "test_create_table_use_policy",
            "dynamic_partition.start" = "-7",
            "abc"="false"
		);
			"""
        assertTrue(false, "should not be able to execute")
	}
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Unknown properties"))
	} finally {
        sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    }



    sql "drop table if exists test_create_table_properties"
    sql """
        CREATE TABLE IF NOT EXISTS test_create_table_properties ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL,
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5)
        PARTITION BY LIST(k1,k2) ( 
            PARTITION p1 VALUES IN (("1","2"),("3","4")), 
            PARTITION p2 VALUES IN (("5","6"),("7","8")), 
            PARTITION p3 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

    sql "drop table if exists test_create_table_properties"
    try {
    sql """
        CREATE TABLE IF NOT EXISTS test_create_table_properties ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL,
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5)
        PARTITION BY LIST(k1,k2) ( 
            PARTITION p1 VALUES IN (("1","2"),("3","4")), 
            PARTITION p2 VALUES IN (("5","6"),("7","8")), 
            PARTITION p3 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 
        properties(
            "replication_num" = "1",
            "abc" = "false"
        )
        """
        assertTrue(false, "should not be able to execute")
	}
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Unknown properties"))
	} finally {
        sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    }


    try {
    sql """
        CREATE TABLE IF NOT EXISTS test_create_table_properties ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL,
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5)
        PARTITION BY LIST(k1,k2) ( 
            PARTITION p1 VALUES IN (("1","2"),("3","4")), 
            PARTITION p2 VALUES IN (("5","6"),("7","8")), 
            PARTITION p3 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 
        properties(
            "replication_num" = "1",
            "storage_policy" = "test_create_table_use_policy",
            "abc" = "false"
        )
        """
        assertTrue(false, "should not be able to execute")
	}
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Unknown properties"))
	} finally {
        sql """ DROP TABLE IF EXISTS test_create_table_properties"""
    }

    sql """
        CREATE TABLE IF NOT EXISTS test_create_table_properties ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL,
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5)
        PARTITION BY LIST(k1,k2) ( 
            PARTITION p1 VALUES IN (("1","2"),("3","4")), 
            PARTITION p2 VALUES IN (("5","6"),("7","8")), 
            PARTITION p3 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 
        properties(
            "replication_num" = "1",
			"storage_policy" = "test_create_table_use_policy"
        )
        """

    sql """ 
    DROP TABLE IF EXISTS test_create_table_properties"""
    sql """
    DROP STORAGE POLICY test_create_table_use_policy;
    """
    sql """
    DROP RESOURCE test_create_table_use_resource
    """

    sql """ DROP TABLE IF EXISTS list_default_par"""
 // create one table without default partition
    sql """
        CREATE TABLE IF NOT EXISTS list_default_par ( 
            k1 INT NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL,
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5)
        PARTITION BY LIST(k1) ( 
            PARTITION p1 VALUES IN ("1","2","3","4"), 
            PARTITION p2 VALUES IN ("5","6","7","8") 
        ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

    // insert value which is not allowed in existing partitions
    try {
        sql """insert into list_default_par values (10,1,1,1,24453.325,1,1)"""
        assertTrue(false, "should not be able to execute")
    }
	catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Insert has filtered data in strict mode"))
	} finally {
    }
    // alter table add default partition
    sql """alter table list_default_par add partition p3"""

    // insert the formerly disallowed value
    sql """insert into list_default_par values (10,1,1,1,24453.325,1,1)"""

    qt_select """select * from list_default_par order by k1"""
    qt_select """select * from list_default_par partition p1 order by k1"""
    qt_select """select * from list_default_par partition p3 order by k1"""

    def bool_tab = "test_short_key_index_bool_tab"
    sql "drop table if exists `${bool_tab}`"
    sql """
         CREATE TABLE IF NOT EXISTS `${bool_tab}` (
          `is_happy` boolean  NOT NULL,
          `k2` VARCHAR(50),
          `k3` DATETIMEV2(6)
        ) ENGINE=OLAP
            DUPLICATE KEY(`is_happy`)
            COMMENT "OLAP"
            PARTITION BY LIST(`is_happy`)
            (
                PARTITION p_happy VALUES IN (1),
                PARTITION p_unhappy VALUES IN (0)
            )
            DISTRIBUTED BY HASH(`k2`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """
    sql """ insert into ${bool_tab} values (1, '2020-12-12 12:12:12', '2000-01-01 12:12:12.123456'), (0, '20201212 121212', '2000-01-01'), (1, '20201212121212', '2000-01-01'), (0, 'AaA', '2000-01-01') """
    result = sql "show partitions from ${bool_tab}"
    logger.info("${result}")
    assertEquals(result.size(), 2)
    
    def tblName1 = "test_range_partition1"
    sql "drop table if exists ${tblName1}"
    sql """
        CREATE TABLE `${tblName1}` (
        `TIME_STAMP` datev2 NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`TIME_STAMP`)
        (PARTITION p20221214 VALUES [('2022-12-14'), ('2022-12-15')),
        PARTITION p20221215 VALUES [('2022-12-15'), ('2022-12-16')),
        PARTITION p20221216 VALUES [('2022-12-16'), ('2022-12-17')),
        PARTITION p20221217 VALUES [('2022-12-17'), ('2022-12-18')),
        PARTITION p20221218 VALUES [('2022-12-18'), ('2022-12-19')),
        PARTITION p20221219 VALUES [('2022-12-19'), ('2022-12-20')),
        PARTITION p20221220 VALUES [('2022-12-20'), ('2022-12-21')),
        PARTITION p20221221 VALUES [('2022-12-21'), ('2022-12-22')))
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into ${tblName1} values ('2022-12-14'), ('2022-12-15'), ('2022-12-16'), ('2022-12-17'), ('2022-12-18'), ('2022-12-19'), ('2022-12-20') """

    qt_select """ select * from ${tblName1} order by TIME_STAMP """
    qt_select """ select * from ${tblName1} WHERE TIME_STAMP = '2022-12-15' order by TIME_STAMP """
    qt_select """ select * from ${tblName1} WHERE TIME_STAMP > '2022-12-15' order by TIME_STAMP """
    qt_select """ select * from ${tblName1} partition p20221220 order by TIME_STAMP"""
    qt_select """ select * from ${tblName1} partition p20221217 order by TIME_STAMP"""
    qt_select """ select * from ${tblName1} partition p20221221 order by TIME_STAMP"""

    sql "drop table if exists ${tblName1}"
    sql """
        CREATE TABLE ${tblName1} (
        `DATA_STAMP` datetime NOT NULL COMMENT '采集时间'
        ) ENGINE=OLAP
        DUPLICATE KEY(`DATA_STAMP`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`DATA_STAMP`)
        (
            PARTITION p0 VALUES [('2023-10-20 00:00:00'), ('2023-10-20 01:00:00')),
            PARTITION p1 VALUES [('2023-10-20 01:00:00'), ('2023-10-20 02:00:00')),
            PARTITION p2 VALUES [('2023-10-20 02:00:00'), ('2023-10-20 03:00:00')),
            PARTITION p3 VALUES [('2023-10-20 03:00:00'), ('2023-10-20 04:00:00')),
            PARTITION p4 VALUES [('2023-10-20 04:00:00'), ('2023-10-20 05:00:00')),
            PARTITION p5 VALUES [('2023-10-20 05:00:00'), ('2023-10-20 06:00:00')),
            PARTITION p6 VALUES [('2023-10-20 06:00:00'), ('2023-10-20 07:00:00')),
            PARTITION p7 VALUES [('2023-10-20 07:00:00'), ('2023-10-20 08:00:00')),
            PARTITION p8 VALUES [('2023-10-20 08:00:00'), ('2023-10-20 09:00:00')),
            PARTITION p9 VALUES [('2023-10-20 09:00:00'), ('2023-10-20 10:00:00')))
        DISTRIBUTED BY HASH(`DATA_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into ${tblName1} values ('2023-10-20 00:00:00'), ('2023-10-20 00:20:00'), ('2023-10-20 00:40:00'), ('2023-10-20 01:00:00'), ('2023-10-20 01:20:00'), ('2023-10-20 01:40:00'), ('2023-10-20 02:00:00'), ('2023-10-20 02:20:00'), ('2023-10-20 02:40:00'), ('2023-10-20 03:00:00'), ('2023-10-20 03:20:00'), ('2023-10-20 03:40:00'), ('2023-10-20 04:00:00'), ('2023-10-20 04:20:00'), ('2023-10-20 04:40:00'), ('2023-10-20 05:00:00'), ('2023-10-20 05:20:00'), ('2023-10-20 05:40:00'), ('2023-10-20 06:00:00'), ('2023-10-20 06:20:00'), ('2023-10-20 06:40:00'), ('2023-10-20 07:00:00'), ('2023-10-20 07:20:00'), ('2023-10-20 07:40:00'), ('2023-10-20 08:00:00'), ('2023-10-20 08:20:00'), ('2023-10-20 08:40:00'), ('2023-10-20 09:00:00'), ('2023-10-20 09:20:00'), ('2023-10-20 09:40:00') """

    qt_select """ select * from ${tblName1} partition p0 order by DATA_STAMP"""
    qt_select """ select * from ${tblName1} partition p5 order by DATA_STAMP"""
    qt_select """ select * from ${tblName1} partition p8 order by DATA_STAMP"""

}