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

suite("test_table_alias") {
     sql """ DROP TABLE IF EXISTS tbl_alias """
     sql """
         CREATE TABLE tbl_alias (
             `id` int
         ) ENGINE=OLAP
         AGGREGATE KEY(`id`)
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`id`) BUCKETS 1
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         );
     """

    try {
        test {
            sql """
            select * 
            from (select t3.id 
                  from (select * from tbl_alias) t1
                 ) t2
            """
            exception "errCode = 2, detailMessage = Unknown column 'id' in 't3'"            
        }
    } finally {
        sql "drop table if exists tbl_alias"
    }
}