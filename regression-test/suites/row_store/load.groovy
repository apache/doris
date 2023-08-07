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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_row_store", "p0") {
    def testTable = "tbl_unique"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
    CREATE TABLE `${testTable}` (
         `tag` varchar(45) NULL,
         `tag_value` varchar(45) NULL,
         `user_ids` decimalv3(30, 8) NULL,
         `test` datetime NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=OLAP
        UNIQUE KEY(`tag`, `tag_value`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`tag`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "store_row_column" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """ 
    sql "insert into ${testTable} (tag,tag_value,user_ids) values ('10001','23',34.234),('10001','23',34.234);"
    sql "insert into ${testTable} (tag,tag_value,user_ids) values ('10001','23',34.234);"
    sql "select * from  ${testTable}"

    testTable = "tbl_dup"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
    CREATE TABLE `${testTable}` (
         `tag` varchar(45) NULL,
         `tag_value` varchar(45) NULL,
         `user_ids` decimalv3(30, 8) NULL,
         `test` datetime NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=OLAP
        UNIQUE KEY(`tag`, `tag_value`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`tag`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "store_row_column" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """
    sql "insert into ${testTable} (tag,tag_value,user_ids) values ('10001','23',34.234),('10001','23',34.234);"
    sql "insert into ${testTable} (tag,tag_value,user_ids) values ('10001','23',34.234);"
    sql "select * from  ${testTable}"
}
