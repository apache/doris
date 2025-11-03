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

suite("test_move_column_with_cast") {
    def tableName = "test_move_column_with_cast"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ 
    CREATE TABLE IF NOT EXISTS ${tableName} (
        k BIGINT,
        v SMALLINT NOT NULL
    ) DUPLICATE KEY(`k`)
      DISTRIBUTED BY HASH(k) BUCKETS 4
      properties("replication_num" = "1");
    """

    sql """ INSERT INTO ${tableName} VALUES(1, 1); """
    sql """ ALTER TABLE ${tableName} ADD COLUMN t2 DATETIME DEFAULT NULL; """
    sql """ ALTER TABLE ${tableName} MODIFY COLUMN v BIGINT AFTER t2; """

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }
}