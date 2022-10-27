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

suite("test_sql_block_rule") {
    sql """
                  DROP SQL_BLOCK_RULE if exists test_rule_num
                """
    sql """
                DROP SQL_BLOCK_RULE if exists test_rule_sql
              """
    sql """
    CREATE TABLE IF NOT EXISTS `table_2` (
      `abcd` varchar(150) NULL COMMENT "",
      `create_time` datetime NULL COMMENT ""
    ) ENGINE=OLAP
    UNIQUE KEY(`abcd`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`abcd`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """
    sql """ INSERT INTO table_2 VALUES ('H220427011909850160918','2022-04-27 16:00:33'),('T220427400109910160949','2022-04-27 16:00:54'),('T220427400123770120058','2022-04-27 16:00:56'),('T220427400126530112854','2022-04-27 16:00:34'),('T220427400127160144672','2022-04-27 16:00:10'),('T220427400127900184511','2022-04-27 16:00:34'),('T220427400129940120380','2022-04-27 16:00:23'),('T220427400139720192986','2022-04-27 16:00:34'),('T220427400140260152375','2022-04-27 16:00:02'),('T220427400153170104281','2022-04-27 16:00:31'),('H220427011909800104411','2022-04-27 16:00:14'),('H220427011909870184823','2022-04-27 16:00:36'),('T220427400115770144416','2022-04-27 16:00:12'),('T220427400126390112736','2022-04-27 16:00:19'),('T220427400128350120717','2022-04-27 16:00:56'),('T220427400129680120838','2022-04-27 16:00:39'),('T220427400136970192083','2022-04-27 16:00:51'),('H220427011909770192580','2022-04-27 16:00:04'),('H220427011909820192943','2022-04-27 16:00:23'),('T220427400109110184990','2022-04-27 16:00:29'),('T220427400109930192249','2022-04-27 16:00:56'),('T220427400123050168464','2022-04-27 16:00:37'),('T220427400124330112931','2022-04-27 16:00:56'),('T220427400124430144718','2022-04-27 16:00:07'),('T220427400130570160488','2022-04-27 16:00:34'),('T220427400130610112671','2022-04-27 16:00:30'),('T220427400137600160704','2022-04-27 16:00:35'),('T220427400144590176969','2022-04-27 16:00:49'),('T220427400146320176530','2022-04-27 16:00:34'),('T220427601780480120027','2022-04-27 16:00:58');"""

    sql """
                CREATE SQL_BLOCK_RULE if not exists test_rule_sql
                PROPERTIES("sql"="SELECT \\\\* FROM table_2", "global"= "true", "enable"= "true")
              """

    test {
        sql("SELECT * FROM table_2", false)
        exception "sql match regex sql block rule: test_rule_sql"
    }

    sql """
                DROP SQL_BLOCK_RULE if exists test_rule_sql
              """

    sql """
                SELECT * FROM table_2
              """
/*
    sql """
                CREATE SQL_BLOCK_RULE if not exists test_rule_num
                PROPERTIES("tablet_num"="1", "global"= "true", "enable"= "true")
              """

    test {
        sql "SELECT * FROM table_2"
        exception "sql hits sql block rule: test_rule_num, reach tablet_num : 1"
    }
*/
    qt_select """
                SHOW SQL_BLOCK_RULE
              """

    sql """
                DROP SQL_BLOCK_RULE if exists test_rule_num
              """

    sql """
                SELECT * FROM table_2
              """

}
