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

// Add PL-SQL regression test notice:
// 1. JDBC does not support the execution of stored procedures that return results. You can only Into the execution
// results into a variable or write them into a table, because when multiple result sets are returned, JDBC needs
// to use the prepareCall statement to execute, otherwise the Statemnt of the returned result executes Finalize.
// Send EOF Packet will report an error;
// 2. The format of the result returned by Doris Statement is xxxx\n, xxxx\n, 2 rows affected (0.03 sec).
// PL-SQL uses Print to print variable values in an unformatted format, and JDBC cannot easily obtain them. Real results.
suite("test_table_options") {
    def dbName = "test_table_options_db"
    sql "drop database if exists ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"

    sql """
	CREATE TABLE IF NOT EXISTS rangetable
	(
    		`user_id` LARGEINT NOT NULL COMMENT "User id",
    		`date` DATE NOT NULL COMMENT "Data fill in date time",
    		`timestamp` DATETIME NOT NULL COMMENT "Timestamp of data being poured",
    		`city` VARCHAR(20) COMMENT "The city where the user is located",
    		`age` SMALLINT COMMENT "User age",
    		`sex` TINYINT COMMENT "User gender",
    		`last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "User last visit time",
    		`cost` BIGINT SUM DEFAULT "0" COMMENT "Total user consumption",
    		`max_dwell_time` INT MAX DEFAULT "0" COMMENT "User maximum dwell time",
    		`min_dwell_time` INT MIN DEFAULT "99999" COMMENT "User minimum dwell time"
	)
	ENGINE=olap
	AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
	PARTITION BY RANGE(`date`)
	(
    		PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
    		PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
    		PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
	)
	DISTRIBUTED BY HASH(`user_id`) BUCKETS 8
	PROPERTIES
	(
    		"replication_num" = "1"
	);
	"""    
    sql """
    	CREATE TABLE IF NOT EXISTS listtable
	(
 	  `user_id` LARGEINT NOT NULL COMMENT "User id",
  	  `date` DATE NOT NULL COMMENT "Data fill in date time",
    	  `timestamp` DATETIME NOT NULL COMMENT "Timestamp of data being poured",
          `city` VARCHAR(20) COMMENT "The city where the user is located",
          `age` SMALLINT COMMENT "User Age",
          `sex` TINYINT COMMENT "User gender",
          `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "User last visit time",
          `cost` BIGINT SUM DEFAULT "0" COMMENT "Total user consumption",
          `max_dwell_time` INT MAX DEFAULT "0" COMMENT "User maximum dwell time",
          `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "User minimum dwell time"
        )
	ENGINE=olap
 	AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
	PARTITION BY LIST(`city`)
	(
    		PARTITION `p_cn` VALUES IN ("Beijing", "Shanghai", "Hong Kong"),
    		PARTITION `p_usa` VALUES IN ("New York", "San Francisco"),
    		PARTITION `p_jp` VALUES IN ("Tokyo")
	)
	DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
	PROPERTIES
	(
    		"replication_num" = "1"
	);
        """
    sql """
        CREATE TABLE IF NOT EXISTS randomtable
	(
    		`user_id` LARGEINT NOT NULL COMMENT "User id",
    		`date` DATE NOT NULL COMMENT "Data fill in date time",
    		`timestamp` DATETIME NOT NULL COMMENT "Timestamp of data being poured",
    		`city` VARCHAR(20) COMMENT "The city where the user is located",
    		`age` SMALLINT COMMENT "User Age",
    		`sex` TINYINT COMMENT "User gender"
	)
	ENGINE=olap
	DISTRIBUTED BY RANDOM BUCKETS 16
	PROPERTIES
	(
    		"replication_num" = "1"
	);
        """
    sql """
	CREATE TABLE IF NOT EXISTS aggregate_table
	(
	    `user_id` LARGEINT NOT NULL COMMENT "user id",
	    `date` DATE NOT NULL COMMENT "data import time",
	    `city` VARCHAR(20) COMMENT "city",
	    `age` SMALLINT COMMENT "age",
	    `sex` TINYINT COMMENT "gender",
	    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "last visit date time",
	    `cost` BIGINT SUM DEFAULT "0" COMMENT "user total cost",
	    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "user max dwell time",
	    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "user min dwell time"
	)
	AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
	DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
	PROPERTIES (
	"replication_allocation" = "tag.location.default: 1"
	);
        """
    sql """
	CREATE TABLE IF NOT EXISTS unique_table
	(
	    `user_id` LARGEINT NOT NULL COMMENT "User ID",
	    `username` VARCHAR(50) NOT NULL COMMENT "Username",
	    `city` VARCHAR(20) COMMENT "User location city",
	    `age` SMALLINT COMMENT "User age",
	    `sex` TINYINT COMMENT "User gender",
	    `phone` LARGEINT COMMENT "User phone number",
	    `address` VARCHAR(500) COMMENT "User address",
	    `register_time` DATETIME COMMENT "User registration time"
	)
	UNIQUE KEY(`user_id`, `username`)
	DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
	PROPERTIES (
	"replication_allocation" = "tag.location.default: 1"
	);
        """
    sql """
	CREATE TABLE IF NOT EXISTS duplicate_table
	(
	    `timestamp` DATETIME NOT NULL COMMENT "Log time",
	    `type` INT NOT NULL COMMENT "Log type",
	    `error_code` INT COMMENT "Error code",
	    `error_msg` VARCHAR(1024) COMMENT "Error detail message",
	    `op_id` BIGINT COMMENT "Operator ID",
	    `op_time` DATETIME COMMENT "Operation time"
	)
	DISTRIBUTED BY HASH(`type`) BUCKETS 1
	PROPERTIES (
	"replication_allocation" = "tag.location.default: 1"
	);
        """
    qt_select """select * from information_schema.table_options where table_schema=\"${dbName}\" order by TABLE_NAME; """
    sql "drop database if exists ${dbName}"
}
