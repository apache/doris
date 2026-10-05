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

suite("test_block_rule_mtmv","mtmv") {
    String suiteName = "test_block_rule_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String ruleName = "${suiteName}_rule"
    sql """drop table if exists `${tableName}`"""
    sql """drop SQL_BLOCK_RULE if exists `${ruleName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    sql"""
        CREATE SQL_BLOCK_RULE ${ruleName}
        PROPERTIES(
          "sql"="select k2 from ${mvName}",
          "global"="true",
          "enable"="true"
        );
        """

    test {
          sql """
               select k2 from ${mvName};
          """
          exception "block rule"
      }

    sql """drop SQL_BLOCK_RULE if exists `${ruleName}`"""
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
