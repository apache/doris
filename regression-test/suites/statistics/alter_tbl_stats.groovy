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

suite("alter_table_stats") {
    def dbName = "test_alter_table_stats"
    def tblName = "alter_stats_tbl"
    def fullTblName = "${dbName}.${tblName}"

    sql """
        DROP DATABASE IF EXISTS ${dbName};
    """

    sql """
        CREATE DATABASE IF NOT EXISTS ${dbName};
    """

    sql """
        DROP TABLE IF EXISTS ${fullTblName};
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${fullTblName} (
            `t_1683700041000_user_id` LARGEINT NOT NULL,
            `t_1683700041000_date` DATEV2 NOT NULL,
            `t_1683700041000_city` VARCHAR(20),
            `t_1683700041000_age` SMALLINT,
            `t_1683700041000_sex` TINYINT,
            `t_1683700041000_last_visit_date` DATETIME REPLACE,
            `t_1683700041000_cost` BIGINT SUM,
            `t_1683700041000_max_dwell_time` INT MAX,
            `t_1683700041000_min_dwell_time` INT MIN
        ) ENGINE=OLAP
        AGGREGATE KEY(`t_1683700041000_user_id`, `t_1683700041000_date`,
         `t_1683700041000_city`, `t_1683700041000_age`, `t_1683700041000_sex`)
        PARTITION BY LIST(`t_1683700041000_date`)
        (
            PARTITION `p_201701` VALUES IN ("2017-10-01"),
            PARTITION `p_201702` VALUES IN ("2017-10-02"),
            PARTITION `p_201703` VALUES IN ("2017-10-03")
        )
        DISTRIBUTED BY HASH(`t_1683700041000_user_id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        ALTER TABLE ${fullTblName} SET STATS ('row_count'='6001215');
    """

    result = sql """
                 SHOW TABLE STATS ${fullTblName};;
             """
    long rowCount = result[0][0] as long
    assert (rowCount == 6001215)
}
