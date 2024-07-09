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

suite("test_schema_change_auto_inc") {
    
    def table1 = "test_schema_change_auto_inc"
    sql "drop table if exists ${table1}"
    sql """
        CREATE TABLE IF NOT EXISTS `${table1}` (
          `name` varchar(65533) NOT NULL,
          `value` int(11) NOT NULL DEFAULT "999",
          `id` BIGINT NOT NULL AUTO_INCREMENT,
          `id2` BIGINT NOT NULL DEFAULT "0"
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`name`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "false",
        "light_schema_change" = "false"
        )"""

    // add an auto-increment column
    test {
        sql "alter table ${table1} add column d BIGINT NULL AUTO_INCREMENT"
        exception "Can not add auto-increment column d"
    }

    // remove auto-increment attribute from a column
    test {
        sql "alter table ${table1} modify column id BIGINT NOT NULL"
        exception "Can't modify the column[id]'s auto-increment attribute."
    }

    // add auto-increment attribute to a column
    test {
        sql "alter table ${table1} modify column id2 BIGINT NOT NULL AUTO_INCREMENT"
        exception "Can't modify the column[id2]'s auto-increment attribute."
    }

    // schema change that invoke double write on a table which has auto-increment column is forbidden 
    test {
        sql "alter table ${table1} modify column value VARCHAR(20) NOT NULL"
        exception "Can not modify column value becasue table ${table1} has auto-increment column id"
    }

    sql """ insert into ${table1}(name, value, id2) values("A", 999, 1), ("B", 888, 2), ("C", 777, 3);"""
    qt_sql "select count(distinct id) from ${table1};"

    sql "alter table ${table1} modify column id BIGINT NOT NULL AUTO_INCREMENT after id2"
    def getJobState = { tbName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    int max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(table1)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    qt_sql "select count(distinct id) from ${table1};"
    sql """ insert into ${table1}(name) values("D"), ("E"), ("F") """
    qt_sql "select count(distinct id) from ${table1};"

    sql "drop table if exists ${table1};"
}
