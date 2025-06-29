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

import org.junit.Assert;

suite("test_create_with_json_object","mtmv") {
    def tableName = "t_test_create_with_json_object"
    def mvName = "test_test_create_with_json_object_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            l_orderkey INTEGER NOT NULL, 
            l_partkey INTEGER NOT NULL, 
            l_suppkey INTEGER NOT NULL, 
            l_linenumber INTEGER NOT NULL, 
            l_ordertime DATETIME NOT NULL, 
            l_quantity DECIMALV3(15, 2) NOT NULL, 
            l_extendedprice DECIMALV3(15, 2) NOT NULL, 
            l_discount DECIMALV3(15, 2) NOT NULL, 
            l_tax DECIMALV3(15, 2) NOT NULL, 
            l_returnflag CHAR(1) NOT NULL, 
            l_linestatus CHAR(1) NOT NULL, 
            l_shipdate DATE NOT NULL, 
            l_commitdate DATE NOT NULL, 
            l_receiptdate DATE NOT NULL, 
            l_shipinstruct CHAR(25) NOT NULL, 
            l_shipmode CHAR(10) NOT NULL, 
            l_comment VARCHAR(44) NOT NULL
        ) DUPLICATE KEY(
            l_orderkey, l_partkey, l_suppkey, 
            l_linenumber
        ) PARTITION BY RANGE(l_ordertime) (
        FROM 
            ('2024-05-01') TO ('2024-06-30') INTERVAL 1 DAY
        )
        DISTRIBUTED BY HASH(l_orderkey) BUCKETS 1 
        properties('replication_num' = '1');
        """
    sql """
        INSERT INTO ${tableName} VALUES      
        (1, 2, 3, 4, '2024-05-01 01:45:05', 5.5, 6.5, 0.1, 8.5, 'o', 'k', '2024-05-01', '2024-05-01', '2024-05-01', 'a', 'b', 'yyyyyyyyy'),    
        (1, 2, 3, 4, '2024-05-15 02:35:05', 5.5, 6.5, 0.15, 8.5, 'o', 'k', '2024-05-15', '2024-05-15', '2024-05-15', 'a', 'b', 'yyyyyyyyy'),     
        (2, 2, 3, 5, '2024-05-25 08:30:06', 5.5, 6.5, 0.2, 8.5, 'o', 'k', '2024-05-25', '2024-05-25', '2024-05-25', 'a', 'b', 'yyyyyyyyy'),     
        (3, 4, 3, 6, '2024-06-02 09:25:07', 5.5, 6.5, 0.3, 8.5, 'o', 'k', '2024-06-02', '2024-06-02', '2024-06-02', 'a', 'b', 'yyyyyyyyy'),     
        (4, 4, 3, 7, '2024-06-15 13:20:09', 5.5, 6.5, 0, 8.5, 'o', 'k', '2024-06-15', '2024-06-15', '2024-06-15', 'a', 'b', 'yyyyyyyyy'),     
        (5, 5, 6, 8, '2024-06-25 15:15:36', 5.5, 6.5, 0.12, 8.5, 'o', 'k', '2024-06-25', '2024-06-25', '2024-06-25', 'a', 'b', 'yyyyyyyyy'),     
        (5, 5, 6, 9, '2024-06-29 21:10:52', 5.5, 6.5, 0.1, 8.5, 'o', 'k', '2024-06-30', '2024-06-30', '2024-06-30', 'a', 'b', 'yyyyyyyyy'),     
        (5, 6, 5, 10, '2024-06-03 22:05:50', 7.5, 8.5, 0.1, 10.5, 'k', 'o', '2024-06-03', '2024-06-03', '2024-06-03', 'c', 'd', 'xxxxxxxxx');     
  
        """
    
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE
        REFRESH AUTO ON COMMIT
        DUPLICATE KEY (`l_orderkey`, `l_partkey`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES ('replication_num' = '1')
        AS
        WITH
        sub_table AS (
            SELECT
                l_orderkey,
                l_partkey,
                CONCAT(
                    '[',
                        GROUP_CONCAT(
                            JSON_OBJECT(
                                'l_linenumber',
                                ${tableName}.l_linenumber,
                                'l_discount',
                                ${tableName}.l_discount
                            )
                        ),
                    ']'
                ) AS l_list
            FROM
                ${tableName}
            GROUP BY
                l_orderkey, l_partkey
            )
        Select * from sub_table;
    """

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO;
    """

    def jobName = getJobName(dbName, mvName);
    log.info(jobName)
    waitingMTMVTaskFinished(jobName)

    order_qt_select "SELECT * FROM ${mvName} where l_orderkey in (2, 3, 4)"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
