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

suite("test_insert_ignore") {

    def tableName = "test_insert_ignore1"
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE;"""
    sql """
            CREATE TABLE ${tableName} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName} values
        (1,"kevin",18,"shenzhen",400),
        (2,"bob",20,"beijing",500),
        (3,"alice",22,"shanghai",600),
        (4,"jack",24,"hangzhou",700),
        (5,"tom",26,"guanzhou",800);"""
    qt_origin_data "select * from ${tableName} order by id;"
    
    // some rows are with existing keys, some are not
    sql """insert ignore into ${tableName} values
        (1,"kevin",18,"shenzhen",4000),
        (10,"alex",28,"shenzhen",1111),
        (2,"bob",20,"beijing",5000),
        (20,"leo",30,"beijing",2222),
        (30,"sam",32,"shanghai",3333),
        (3,"alice",22,"shanghai",6000),
        (4,"jack",24,"hangzhou",7000),
        (40,"Ruth",34,"hangzhou",4444),
        (5,"tom",26,"guanzhou",8000),
        (50,"cynthia",36,"guanzhou",8000);"""

    qt_after_insert_ignore "select * from ${tableName} order by id;"
    sql """ DROP TABLE IF EXISTS ${tableName};"""

    def tableName2 = "test_insert_ignore2"
    sql """ DROP TABLE IF EXISTS ${tableName2} FORCE; """
    sql """CREATE TABLE IF NOT EXISTS ${tableName2} (
                `uid` BIGINT NULL,
                `v1` BIGINT NULL 
                )
    UNIQUE KEY(uid)
    DISTRIBUTED BY HASH(uid) BUCKETS 1
    PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1"
    );"""

    sql "insert into ${tableName2} values(1,1);"
    qt_origin_data "select * from ${tableName2} order by uid;"

    sql "insert into ${tableName2}(uid, v1, __DORIS_DELETE_SIGN__) values(1, 2, 1);"
    qt_delete_a_row "select * from ${tableName2} order by uid;"

    sql "insert ignore into ${tableName2} values(1,3);"
    qt_after_insert_ignore "select * from ${tableName2} order by uid;"

    sql "insert into ${tableName2} values(1,10);"
    qt_sql "select * from ${tableName2} order by uid;"

    sql "insert into ${tableName2}(uid, v1, __DORIS_DELETE_SIGN__) values(1, 1, 1);"
    qt_delete_a_row "select * from ${tableName2} order by uid;"

    sql "insert ignore into ${tableName2} values(1,1);"
    qt_after_insert_ignore "select * from ${tableName2} order by uid;"
    sql """ DROP TABLE IF EXISTS ${tableName2}; """


    // test illigal cases
    def tableName3 = "test_insert_ignore3"
    sql """ DROP TABLE IF EXISTS ${tableName3} FORCE; """
    sql """CREATE TABLE IF NOT EXISTS ${tableName3} (
                `uid` BIGINT NULL,
                `v1` BIGINT NULL 
            ) UNIQUE KEY(uid)
    DISTRIBUTED BY HASH(uid) BUCKETS 1
    PROPERTIES (
        "enable_unique_key_merge_on_write" = "false",
        "replication_num" = "1"
    );"""
    sql "insert into ${tableName3} values(1,1);"
    test {
        sql "insert ignore into ${tableName3} values(1,3);"
        exception "ignore mode can only be enabled if the target table is a unique table with merge-on-write enabled."
    }

    def tableName4 = "test_insert_ignore4"
    sql """ DROP TABLE IF EXISTS ${tableName4} FORCE; """
    sql """CREATE TABLE IF NOT EXISTS ${tableName4} (
                `uid` BIGINT NULL,
                `v1` BIGINT NULL 
            ) UNIQUE KEY(uid)
    DISTRIBUTED BY HASH(uid) BUCKETS 1
    PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1",
        "function_column.sequence_col" = 'v1'
    );"""
    sql "insert into ${tableName4} values(1,1);"
    test {
        sql "insert ignore into ${tableName4} values(1,3);"
        exception "ignore mode can't be used if the target table has sequence column, but table[${tableName4}] has sequnce column."
    }
}
