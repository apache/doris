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

suite("test_insert") {
    // todo: test insert, such as insert values, insert select, insert txn
    sql "show load"
    def test_baseall = "baseall"
    def test_bigtable = "bigtable"
    def insert_tbl = "test_insert_tbl"

    sql """ DROP TABLE IF EXISTS ${insert_tbl}"""
    sql """
     CREATE TABLE ${insert_tbl} (
       `k1` char(5) NULL,
       `k2` int(11) NULL,
       `k3` tinyint(4) NULL,
       `k4` int(11) NULL
     ) ENGINE=OLAP
     DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`)
     COMMENT 'OLAP'
     DISTRIBUTED BY HASH(`k1`) BUCKETS 5
     PROPERTIES (
       "replication_num"="1"
     );
    """

    sql """ 
    INSERT INTO ${insert_tbl}
    SELECT a.k6, a.k3, b.k1
    	, sum(b.k1) OVER (PARTITION BY a.k6 ORDER BY a.k3) AS w_sum
    FROM ${test_baseall} a
    	JOIN ${test_bigtable} b ON a.k1 = b.k1 + 5
    ORDER BY a.k6, a.k3, a.k1, w_sum
    """

    qt_sql1 "select * from ${insert_tbl} order by 1, 2, 3, 4"

    def insert_tbl_dft = "test_insert_dft2_tbl"
    sql """ DROP TABLE IF EXISTS ${insert_tbl_dft}"""
    
    // `k7` should be float type, and bug exists now, https://github.com/apache/doris/pull/20867
    // `k9` should be char(16), and bug exists now as error msg raised:"can not cast from origin type TINYINT to target type=CHAR(16)" when doing insert
    // "`k13` datetime default CURRENT_TIMESTAMP" might have cast error in strict mode when doing insert:
    // [INTERNAL_ERROR]Invalid value in strict mode for function CAST, source column String, from type String to type DateTimeV2
    sql """
        CREATE TABLE ${insert_tbl_dft} (
            `k1` boolean default "true",
            `k2` tinyint default "10",
            `k3` smallint default "10000",
            `k4` int default "10000000",
            `k5` bigint default "92233720368547758",
            `k6` largeint default "19223372036854775807",
            	  
            `k8` double default "3.14159",

            `k10` varchar(64) default "hello world, today is 15/06/2023",
            `k11` date default "2023-06-15",
            `k12` datetime default "2023-06-15 16:10:15" 
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
            "replication_num"="1"
        );
    """

    sql """ insert into ${insert_tbl_dft} values() """
    sql """ insert into ${insert_tbl_dft} values() """
    
    qt_select """ select k1, k2, k3, k4, k5, k6, k8, k10, k11, k12 from ${insert_tbl_dft} """
    
    sql 'drop table if exists t3'
    sql '''
        create table t3 (
            id int default "10"
        ) distributed by hash(id) buckets 13
        properties(
            'replication_num'='1'
        );
    '''
    
    sql 'insert into t3 values(default)'
    
    test {
        sql 'select * from t3'
        result([[10]])
    }
}
