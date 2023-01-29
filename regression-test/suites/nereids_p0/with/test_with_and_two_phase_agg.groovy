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

suite("test_with_and_two_phase_agg") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test_with_and_two_phase_agg_table"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}(
            `key` int not null,
            `key2` varchar(50) not null,
            `account` varchar(50) not null
        ) ENGINE = OLAP
        UNIQUE KEY (`key`, `key2`)
        DISTRIBUTED BY HASH(`key`)
        PROPERTIES("replication_num" = "1");
    """
    sql """ INSERT INTO ${tableName} VALUES (1, '1332050726', '1332050726'); """
    qt_select """
                WITH t2 AS( SELECT  sum(`key`) num, COUNT(DISTINCT `account`) unt
                FROM ${tableName}) SELECT num FROM t2;
              """
    qt_select2 """
                 WITH t2 AS( SELECT `key2`, sum(`key`) num, COUNT(DISTINCT `account`) unt
                 FROM ${tableName} GROUP BY `key2`) SELECT num FROM t2;
              """
}
