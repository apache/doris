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

suite("test_sql_block_rule_status") {
    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_sql_block_rule_status"
    String tableName = "${suiteName}_table"
    String blockRuleName = "${suiteName}_rule"
    sql """
        drop SQL_BLOCK_RULE if exists ${blockRuleName};
    """
     sql """
        drop table if exists ${tableName};
    """
    sql """
        CREATE TABLE ${tableName}
        (
            k1 INT,
            k2 varchar(32)
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE SQL_BLOCK_RULE ${blockRuleName}
        PROPERTIES(
        "sql"="select k1,k2 from ${dbName}.${tableName}",
        "global"="true",
        "enable"="true"
        );
    """

    test {
          sql """
              select k1,k2 from ${dbName}.${tableName};
          """
          exception "sql match"
      }
    order_qt_count "SELECT count(*) FROM information_schema.sql_block_rule_status where name ='${blockRuleName}'"
    order_qt_select "SELECT NAME,PATTERN,SQL_HASH,PARTITION_NUM,TABLET_NUM,CARDINALITY,GLOBAL,ENABLE,BLOCKS FROM information_schema.sql_block_rule_status where name ='${blockRuleName}'"
     sql """
        drop SQL_BLOCK_RULE if exists ${blockRuleName};
    """
     sql """
        drop table if exists ${tableName};
    """
}

