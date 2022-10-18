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


/*
The varchar column type of the view needs to be a specific number, not an asterisk
Expect: varchar(32)
Type before bug fix: varchar(*)
*/
 suite("test_view_varchar_length") {
     def tableName = "t_view_varchar_length";
     def viewName = "v_test_view_varchar_length";

     sql """ DROP TABLE IF EXISTS ${tableName} """
     sql """
         CREATE TABLE ${tableName} (
             `id` int,
             `name` varchar(32) 
         ) ENGINE=OLAP
         COMMENT "OLAP"
         DISTRIBUTED BY HASH(`id`) BUCKETS 1
         PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "in_memory" = "false",
             "storage_format" = "V2"
         );
     """
     sql "drop view if exists ${viewName};"
     sql """
         create view ${viewName} as select name from ${tableName};
     """

     qt_sql """
         desc v_test_view_varchar_length;
     """

     sql "DROP VIEW ${viewName}"
     sql "DROP TABLE ${tableName}"
 }

