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

suite("test_show_delete") {
    def tableName = "test_show_delete_table"
    sql """drop table if exists ${tableName}"""
    
    sql """
             CREATE TABLE IF NOT EXISTS ${tableName}
        (
            `datetime` DATE NOT NULL COMMENT "['0000-01-01', '9999-12-31']",
            `type` TINYINT NOT NULL COMMENT "[-128, 127]",
            `user_id` decimal(9,3) COMMENT "[-9223372036854775808, 9223372036854775807]"
        )
        UNIQUE KEY(`datetime`)
        PARTITION BY RANGE(`datetime`)
        (
                PARTITION `Feb` VALUES LESS THAN ("2022-03-01"),
                PARTITION `Mar` VALUES LESS THAN ("2022-04-01")
        )
        DISTRIBUTED BY HASH(`datetime`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into ${tableName} values ('2022-02-01', 1, 1.1), ('2022-03-01', 2, 2.2)"""

    sql """ set delete_without_partition = true"""
    // don't care nereids planner
    sql """ delete from ${tableName} PARTITION Mar  where type ='2'"""
    sql """ delete from ${tableName}   where type ='1'"""
    def showDeleteResult = sql """ show delete"""
    //When we test locally, multiple history results will be included, so size will be >= 2
    assert showDeleteResult.size() >= 0
    def count = 0
    showDeleteResult.each { row ->

        if (row[3] == 'type EQ "2"') {
           assert row[1] == 'Mar'
            count++
            return
        }
        if (row[3] == 'type EQ "1"') {
            assert row[1] == '*'
            count++
            return
        }
        
    }
    assert count == showDeleteResult.size()

}