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
How to produce the bug:
suppose we have table T, and a view V: select * from T where T.id>0
When we execute sql: select * from V as v1 join V as v2 on v1.id=v2.id where v1.id in (1,2);
by InferFilterRule, { v1.id=v2.id , v1.id in (1,2) } => v2.id in (1,2)
and then we push v1.id in (1,2) and v2.id in (1,2) down to v1 and v2, respectively.

In re-analyze phase, we expand v1 with infered condition, we have sql: select * from T where T.id>0 and v1.id in (1,2)
The bug is we cannot resolve v1.id in context of the expanded sql.
The same resolve error occurs when re-analyze v2.
*/
 suite("test_pushdown_pred_to_view") {
     def tableName = "t_pushdown_pred_to_view";
     def viewName = "v_pushdown_pred_to_view";
     sql """ DROP TABLE IF EXISTS ${tableName} """
     sql """
         CREATE TABLE ${tableName} (
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
     sql "drop view if exists ${viewName};"
     sql """
         create view ${viewName} as select * from ${tableName} where id > 0;
     """

     sql """
         insert into ${tableName} values(1);
     """

     qt_sql """
         select * from ${viewName} as v1 join ${viewName} as v2 on v1.id=v2.id and v1.id>0;
     """
     sql "DROP VIEW ${viewName}"
     sql "DROP TABLE ${tableName}"
 }

