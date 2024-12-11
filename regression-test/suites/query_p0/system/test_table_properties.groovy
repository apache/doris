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

suite("test_table_properties") {
    def dbName = "test_table_properties_db"
    sql "drop database if exists ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"

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

    qt_select_check_1 """select count(*) from information_schema.table_properties where table_schema=\"${dbName}\"; """
    qt_select_check_2 """select * from information_schema.table_properties where table_schema=\"${dbName}\" and PROPERTY_NAME != "default.replication_allocation" ORDER BY TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PROPERTY_NAME,PROPERTY_VALUE"""
    sql """
        drop table listtable;
    """    
    qt_select_check_3 """select * from information_schema.table_properties where table_schema=\"${dbName}\" and PROPERTY_NAME != "default.replication_allocation" ORDER BY TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PROPERTY_NAME,PROPERTY_VALUE"""

    def user = "table_properties_user"
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123abc!@#'"
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }	
    sql "GRANT SELECT_PRIV ON information_schema.table_properties  TO ${user}"
    
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"

    connect(user, '123abc!@#', url) {
       qt_select_check_4 """select * from information_schema.table_properties where PROPERTY_NAME != "default.replication_allocation" ORDER BY TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PROPERTY_NAME,PROPERTY_VALUE"""
    }

    sql "GRANT SELECT_PRIV ON ${dbName}.duplicate_table  TO ${user}"
    connect(user, '123abc!@#', url) {
       qt_select_check_5 """select * from information_schema.table_properties where PROPERTY_NAME != "default.replication_allocation" ORDER BY TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PROPERTY_NAME,PROPERTY_VALUE"""
    }
 
    sql "REVOKE SELECT_PRIV ON ${dbName}.duplicate_table  FROM ${user}"
    connect(user, '123abc!@#', url) {
       qt_select_check_6 """select * from information_schema.table_properties where PROPERTY_NAME != "default.replication_allocation" ORDER BY TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PROPERTY_NAME,PROPERTY_VALUE"""
    }



}
