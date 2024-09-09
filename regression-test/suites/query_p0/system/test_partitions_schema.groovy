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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_partitions_schema") {
    def dbName = "test_partitions_schema_db"
    def listOfColum = "TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME,SUBPARTITION_NAME,PARTITION_ORDINAL_POSITION,SUBPARTITION_ORDINAL_POSITION,PARTITION_METHOD,SUBPARTITION_METHOD,PARTITION_EXPRESSION,SUBPARTITION_EXPRESSION,PARTITION_DESCRIPTION,TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE,CHECKSUM,PARTITION_COMMENT,NODEGROUP,TABLESPACE_NAME";
    sql "drop database if exists ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"
 
     def checkRowCount = { expectedRowCount ->
        Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until(
            {
                def result = sql "select table_rows from  information_schema.partitions where table_name='test_range_table' and partition_name='p0'"
                logger.info("table: table_name, rowCount: ${result}")
                return result[0][0] == expectedRowCount
            }
        )
    }
    
    sql """
        create table test_range_table (
        col_1 int,
        col_2 int,
        col_3 int,
        col_4 int,
        pk int
        ) engine=olap
        DUPLICATE KEY(col_1, col_2)
        PARTITION BY             RANGE(col_1) (
                        PARTITION p0 VALUES LESS THAN ('4'),
                        PARTITION p1 VALUES LESS THAN ('6'),
                        PARTITION p2 VALUES LESS THAN ('7'),
                        PARTITION p3 VALUES LESS THAN ('8'),
                        PARTITION p4 VALUES LESS THAN ('10'),
                        PARTITION p5 VALUES LESS THAN ('83647'),
                        PARTITION p100 VALUES LESS THAN ('2147483647')
                    )
                
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into test_range_table(pk,col_1,col_2,col_3,col_4) values (0,6,-179064,5213411,5),(1,3,5,2,6),(2,4226261,7,null,3),(3,9,null,4,4),(4,-1003770,2,1,1),(5,8,7,null,8176864),(6,3388266,5,8,8),(7,5,1,2,null),(8,9,2064412,0,null),(9,1489553,8,-446412,6),(10,1,3,0,1),(11,null,3,4621304,null),(12,null,-3058026,-262645,9),(13,null,null,9,3),(14,null,null,5037128,7),(15,299896,-1444893,8,1480339),(16,7,7,0,1470826),(17,-7378014,5,null,5),(18,0,3,6,5),(19,5,3,-4403612,-3103249);
    """
    sql """
        sync
    """
    checkRowCount(9);
 
    qt_select_check_0 """select  table_name,partition_name,table_rows from information_schema.partitions where table_schema=\"${dbName}\" order by $listOfColum"""
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
    PARTITION BY LIST(user_id, city)
    (
        PARTITION p1_city VALUES IN (("1", "Beijing"), ("1", "Shanghai")),
        PARTITION p2_city VALUES IN (("2", "Beijing"), ("2", "Shanghai")),
        PARTITION p3_city VALUES IN (("3", "Beijing"), ("3", "Shanghai"))
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
	
    // test row column page size
    sql """
        CREATE TABLE IF NOT EXISTS test_row_column_page_size1 (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "store_row_column" = "true"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS test_row_column_page_size2 (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "store_row_column" = "true",
            "row_store_page_size" = "8190"
        );
    """
    qt_select_check_1 """select  $listOfColum from information_schema.partitions where table_schema=\"${dbName}\" order by $listOfColum"""
    sql """
        drop table test_row_column_page_size2;
    """    
    qt_select_check_2 """select  $listOfColum from information_schema.partitions where table_schema=\"${dbName}\" order by $listOfColum"""

    def user = "partitions_user"
    sql "DROP USER IF EXISTS ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123abc!@#'"
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    sql "GRANT SELECT_PRIV ON information_schema.partitions  TO ${user}"

    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"

    connect(user=user, password='123abc!@#', url=url) {
           qt_select_check_3 """select  $listOfColum from information_schema.partitions where table_schema=\"${dbName}\" order by $listOfColum"""
    }

    sql "GRANT SELECT_PRIV ON ${dbName}.duplicate_table  TO ${user}"
    connect(user=user, password='123abc!@#', url=url) {
        order_qt_select_check_4 """select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME from information_schema.partitions where table_schema=\"${dbName}\""""
    }
    
    sql "REVOKE SELECT_PRIV ON ${dbName}.duplicate_table  FROM ${user}"
    connect(user=user, password='123abc!@#', url=url) {
        order_qt_select_check_5 """select TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PARTITION_NAME from information_schema.partitions where table_schema=\"${dbName}\""""
    }

}
