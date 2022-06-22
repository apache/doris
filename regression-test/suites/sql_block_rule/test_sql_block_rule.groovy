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

suite("test_sql_block_rule", "sql_block_rule") {
    sql """
                USE ${context.config.defaultDb}
              """

    sql """
                CREATE SQL_BLOCK_RULE test_rule_sql
                PROPERTIES("sql"="SELECT \\\\* FROM table_2", "global"= "true", "enable"= "true")
              """

    test {
        sql "SELECT * FROM table_2"
        exception "sql match regex sql block rule: test_rule_sql"
    }

    sql """
                DROP SQL_BLOCK_RULE test_rule_sql
              """

    sql """
                SELECT * FROM table_2
              """

    sql """
                CREATE SQL_BLOCK_RULE test_rule_num
                PROPERTIES("tablet_num"="1", "global"= "true", "enable"= "true")
              """

    test {
        sql "SELECT * FROM table_2"
        exception "sql hits sql block rule: test_rule_num, reach tablet_num : 1"
    }

    qt_select """
                SHOW SQL_BLOCK_RULE
              """

    sql """
                DROP SQL_BLOCK_RULE test_rule_num
              """

    sql """
                SELECT * FROM table_2
              """

}
