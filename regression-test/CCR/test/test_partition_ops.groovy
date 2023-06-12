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

suite("test_partiton_ops") {

    def syncer = getSyncer()
    def tableName = "tbl_partiton_ops"
    def test_num = 0
    def insert_num = 5

    sql "DROP TABLE IF EXISTS ${txnTableName}"
    sql """
           CREATE TABLE if NOT EXISTS ${txnTableName} 
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1 
           PROPERTIES ( 
               "replication_allocation" = "tag.location.default: 1"
           )
        """
    sql """ALTER TABLE ${txnTableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${txnTableName}"
    target_sql """
                  CREATE TABLE if NOT EXISTS ${txnTableName} 
                  (
                      `test` INT,
                      `id` INT
                  )
                  ENGINE=OLAP
                  UNIQUE KEY(`test`, `id`)
                  DISTRIBUTED BY HASH(id) BUCKETS 1 
                  PROPERTIES ( 
                      "replication_allocation" = "tag.location.default: 1"
                  )
              """
    assertTrue(syncer.getTargetMeta("${txnTableName}"))
}