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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("agg_no_group") {
    sql "DROP TABLE IF EXISTS lineitem_2_agg"
    sql """
            CREATE TABLE `lineitem_2_agg` (
            `l_orderkey` BIGINT NULL,
            `l_linenumber` INT NULL,
            `l_partkey` INT NULL,
            `l_suppkey` INT NULL,
            `l_shipdate` DATE not NULL,
            `l_quantity` DECIMAL(15, 2) sum,
            `l_extendedprice` DECIMAL(15, 2) sum,
            `l_discount` DECIMAL(15, 2) sum,
            `l_tax` DECIMAL(15, 2) sum,
            `l_returnflag` VARCHAR(1) replace,
            `l_linestatus` VARCHAR(1) replace,
            `l_commitdate` DATE replace,
            `l_receiptdate` DATE replace,
            `l_shipinstruct` VARCHAR(25) replace,
            `l_shipmode` VARCHAR(10) replace,
            `l_comment` VARCHAR(44) replace
            ) ENGINE=OLAP
            aggregate KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate )
            COMMENT 'OLAP'
            auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
            DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
    sql """insert into lineitem_2_agg values 
                (null, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
                (1, null, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
                (3, 3, null, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
                (1, 2, 3, null, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
                (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
                (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
                (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');"""

    test {
        sql """CREATE MATERIALIZED VIEW mv_name_2_3_5  AS  
        select l_shipdate, l_partkey, l_orderkey from lineitem_2_agg"""
        exception "agg mv must has group by clause"
    }
}
